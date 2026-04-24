package fuku

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chenhg5/cc-connect/core"
	"github.com/gorilla/websocket"
)

func init() {
	core.RegisterPlatform("fuku", New)
}

// Platform connects to a OneBot v11 implementation (NapCat, LLOneBot, etc.)
// via forward WebSocket. It receives message events and sends messages back
// through the same WS connection.
type Platform struct {
	wsURL                 string // e.g. "ws://127.0.0.1:3001"
	token                 string // optional access_token
	allowFrom             string // comma-separated user IDs or "*"
	shareSessionInChannel bool
	handler               core.MessageHandler
	conn                  *websocket.Conn
	mu                    sync.Mutex
	echoSeq               atomic.Int64
	echoCh                sync.Map // echo -> chan json.RawMessage
	cancel                context.CancelFunc
	selfID                int64
	dedup                 core.MessageDedup
	groupNameCache        sync.Map // groupID -> group name
}

func New(opts map[string]any) (core.Platform, error) {
	wsURL, _ := opts["ws_url"].(string)
	if wsURL == "" {
		// 本地宿主机用127.0.0.1不行，需要使用localhost。
		//wsURL = "ws://localhost:3000/ws?conversation_id=" + os.Getenv("CONVERSATION_ID") + "&is_container=true"
		// 本地容器内配置
		wsURL = "ws://host.docker.internal:3000/ws?conversation_id=" + os.Getenv("CONVERSATION_ID") + "&is_container=true"
		// 线上容器内配置
		// wsURL = "ws://chat.zipclaw.org/ws?conversation_id=" + os.Getenv("CONVERSATION_ID") + "&is_container=true"
	}
	token, _ := opts["token"].(string)
	allowFrom, _ := opts["allow_from"].(string)
	shareSessionInChannel, _ := opts["share_session_in_channel"].(bool)

	core.CheckAllowFrom("fuku", allowFrom)
	return &Platform{
		wsURL:                 wsURL,
		token:                 token,
		allowFrom:             allowFrom,
		shareSessionInChannel: shareSessionInChannel,
	}, nil
}

func (p *Platform) Name() string { return "fuku" }

func (p *Platform) Start(handler core.MessageHandler) error {
	p.handler = handler

	header := http.Header{}
	if p.token != "" {
		header.Set("Authorization", "Bearer "+p.token)
	}

	conn, _, err := websocket.DefaultDialer.Dial(p.wsURL, header)
	if err != nil {
		return fmt.Errorf("fuku: ws connect failed (%s): %w", p.wsURL, err)
	}
	p.conn = conn

	slog.Info("fuku: connected to OneBot", "url", p.wsURL)

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel

	go p.readLoop(ctx)

	return nil
}

func (p *Platform) readLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_, raw, err := p.conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("fuku: ws read error, reconnecting...", "error", err)
			p.reconnect()
			continue
		}

		var payload map[string]any
		if json.Unmarshal(raw, &payload) != nil {
			continue
		}
		//slog.Info("fuku: ws read message", "raw", string(raw))
		slog.Info("fuku: ws read message2", "payload", payload)

		// If this is an API response (has "echo" field), route to caller
		if echo, ok := payload["echo"].(string); ok {
			if ch, loaded := p.echoCh.LoadAndDelete(echo); loaded {
				if dataCh, ok := ch.(chan json.RawMessage); ok {
					dataCh <- raw
				}
			}
			continue
		}

		// Otherwise it's an event
		postType, _ := payload["type"].(string)
		if postType == "message" || postType == "audio" {
			p.handleMessage(payload)
		}
	}
}

func (p *Platform) reconnect() {
	for i := 1; i <= 30; i++ {
		time.Sleep(time.Duration(i) * 2 * time.Second)
		header := http.Header{}
		if p.token != "" {
			header.Set("Authorization", "Bearer "+p.token)
		}
		conn, _, err := websocket.DefaultDialer.Dial(p.wsURL, header)
		if err != nil {
			slog.Warn("fuku: reconnect attempt failed", "attempt", i, "error", err)
			continue
		}
		p.mu.Lock()
		p.conn = conn
		p.mu.Unlock()
		slog.Info("fuku: reconnected")
		return
	}
	slog.Error("fuku: failed to reconnect after 30 attempts")
}

func (p *Platform) handleMessage(payload map[string]any) {
	msgType, _ := payload["type"].(string)
	conversationId, _ := payload["conversation_id"].(string)
	messageID := jsonInt64(payload, "message_id")
	//userID := jsonInt64(payload, "user_id")
	// TODO: 一个cc-connect是可以连接多个用户的，可以多seesion，后面研究下怎么用，结合我们的业务有没有啥用。
	// 目前我是一个对话一个容器，容器里启动一个cc-connect+claude，所有，都设置一样的也没问题。
	userID := int64(123)
	//
	//if userID == p.selfID {
	//	return
	//}

	if ts, ok := payload["time"].(float64); ok && ts > 0 {
		if core.IsOldMessage(time.Unix(int64(ts), 0)) {
			slog.Debug("fuku: ignoring old message after restart", "time", int64(ts))
			return
		}
	}

	msgIDStr := strconv.FormatInt(messageID, 10)
	if p.dedup.IsDuplicate(msgIDStr) {
		slog.Debug("fuku: duplicate message ignored", "message_id", messageID)
		return
	}

	//if !p.isAllowed(userID) {
	//	return
	//}

	// Parse message content from CQ message array or raw_message
	text, images, audio := p.parseMessage(payload)
	if text == "" && len(images) == 0 && audio == nil {
		slog.Debug("fuku: ignoring message ignored", "message_id", messageID)
		return
	}

	var sessionKey string
	sessionKey = fmt.Sprintf("fuku:%d", userID)

	rctx := &replyContext{
		messageType:    msgType,
		userID:         userID,
		messageID:      int32(messageID),
		conversationId: conversationId,
	}

	msg := &core.Message{
		SessionKey: sessionKey,
		Platform:   "fuku",
		MessageID:  strconv.FormatInt(messageID, 10),
		UserID:     strconv.FormatInt(userID, 10),
		Content:    text,
		Images:     images,
		Audio:      audio,
		ReplyCtx:   rctx,
	}

	slog.Debug("fuku: message received", "type", msgType, "user", userID, "text_len", len(text))
	p.handler(p, msg)
}

// Reply sends a message as a reply to an incoming message.
func (p *Platform) Reply(ctx context.Context, replyCtx any, content string) error {
	return p.Send(ctx, replyCtx, content)
}

// Send sends a message to the conversation identified by replyCtx.
func (p *Platform) Send(ctx context.Context, replyCtx any, content string) error {
	rctx, ok := replyCtx.(*replyContext)
	if !ok {
		return fmt.Errorf("fuku: invalid reply context")
	}

	params := map[string]any{
		"message": content,
	}

	params["user_id"] = rctx.userID
	_, err := p.callAPI("send_private_msg", params)
	return err
}

func (p *Platform) callAPI(action string, params map[string]any) (map[string]any, error) {
	seq := p.echoSeq.Add(1)
	echo := strconv.FormatInt(seq, 10)

	req := map[string]any{
		"action": action,
		"echo":   echo,
	}
	if params != nil {
		req["params"] = params
	}

	ch := make(chan json.RawMessage, 1)
	p.echoCh.Store(echo, ch)
	defer p.echoCh.Delete(echo)

	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	err = p.conn.WriteMessage(websocket.TextMessage, data)
	p.mu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("fuku: ws write: %w", err)
	}

	select {
	case raw := <-ch:
		var resp struct {
			Status  string          `json:"status"`
			RetCode int             `json:"retcode"`
			Data    json.RawMessage `json:"data"`
		}
		if json.Unmarshal(raw, &resp) != nil {
			return nil, fmt.Errorf("fuku: invalid API response")
		}
		if resp.RetCode != 0 {
			return nil, fmt.Errorf("fuku: API %s failed (retcode=%d)", action, resp.RetCode)
		}
		var result map[string]any
		_ = json.Unmarshal(resp.Data, &result)
		return result, nil

	case <-time.After(15 * time.Second):
		return nil, fmt.Errorf("fuku: API %s timeout", action)
	}
}

func (p *Platform) Stop() error {
	if p.cancel != nil {
		p.cancel()
	}
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}

func (p *Platform) parseMessage(payload map[string]any) (string, []core.ImageAttachment, *core.AudioAttachment) {
	var textParts []string
	var images []core.ImageAttachment
	var audio *core.AudioAttachment
	slog.Info("fuku: parseMessage", "payload", payload)

	defer func() {
		if r := recover(); r != nil {
			slog.Error("fuku: panic recovered in readLoop", "panic", r)
			// 可以在这里添加堆栈打印，方便调试
			debug.PrintStack()
		}
	}()
	msgType, _ := payload["type"].(string)
	switch msgType {
	case "message":
		// 目前fuku只有文本消息
		contentVal := payload["content"]
		if contentVal != nil {
			if contentStr, ok := contentVal.(string); ok && contentStr != "" {
				textParts = append(textParts, contentStr)
			}
		}
	case "audio":
		filePath, _ := payload["content"].(string)
		name := strings.TrimSuffix(filepath.Base(filePath), filepath.Ext(filePath))
		filePath = fmt.Sprintf("/Users/zhangzhen/zzworkspace/claude_ws/audios/%s.mp3", name)
		// fuku: parsing audio filePath: 31598678388778@lid_3AEB207FAD34F75516B1_5
		fmt.Printf("fuku: parsing audio filePath: %s\n", name)
		// 读取audio文件
		file, err := os.Open(filePath)
		if err != nil {
			fmt.Println("打开文件失败:", err)
			break
		}
		defer func(file *os.File) {
			_ = file.Close()
		}(file)

		audioData, err := io.ReadAll(file)
		if err != nil {
			fmt.Printf("%s: read resource err: %v", filePath, err)
		}

		audio = &core.AudioAttachment{
			MimeType: "audio/mpeg",
			Data:     audioData,
			Format:   "mp3",
		}

		duration, err := extractDuration(name)
		if err != nil {
			fmt.Printf("从 %s 提取时长失败: %v\n", name, err)
		} else if duration > 0 {
			audio.Duration = duration
		}
	}

	return strings.TrimSpace(strings.Join(textParts, "")), images, audio
}

func extractDuration(name string) (int, error) {
	parts := strings.Split(name, "_")

	if len(parts) == 0 {
		return 0, fmt.Errorf("文件名格式错误")
	}

	return strconv.Atoi(parts[len(parts)-1])
}

func jsonInt64(m map[string]any, key string) int64 {
	switch v := m[key].(type) {
	case float64:
		return int64(v)
	case json.Number:
		n, _ := v.Int64()
		return n
	}
	return 0
}

type replyContext struct {
	messageType    string // "private" or "group"
	userID         int64
	groupID        int64
	messageID      int32
	conversationId string
}

func (p *Platform) ReconstructReplyCtx(sessionKey string) (any, error) {
	// 前面有设置：
	//sessionKey = fmt.Sprintf("fuku:%d", userID)

	// 下面参考qq里的写
	parts := strings.SplitN(sessionKey, ":", 2)
	if len(parts) < 2 || parts[0] != "fuku" {
		return nil, fmt.Errorf("fuku: invalid session key %q", sessionKey)
	}
	uid, _ := strconv.ParseInt(parts[1], 10, 64)
	return &replyContext{messageType: "private", userID: uid}, nil
}
