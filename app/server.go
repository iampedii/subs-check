package app

import (
	"bytes"
	"crypto/subtle"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/beck-8/subs-check/check"
	"github.com/beck-8/subs-check/config"
	"github.com/beck-8/subs-check/save/method"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"gopkg.in/yaml.v3"
)

// initHttpServer initializes the HTTP server.
func (app *App) initHttpServer() error {
	gin.SetMode(gin.ReleaseMode)
	// Route gin's access log and panic stacks into their own temp file
	// (same directory as the app log but a separate file). Keeps stdout
	// clean for the CLI progress renderer and keeps the web UI's log
	// viewer free of HTTP request noise. gin.Default() snapshots
	// DefaultWriter / DefaultErrorWriter at call time, so assign first.
	gin.DefaultWriter = GinFileLogger
	gin.DefaultErrorWriter = GinFileLogger
	router := gin.Default()

	saver, err := method.NewLocalSaver()
	if err != nil {
		return fmt.Errorf("failed to get HTTP listen directory: %w", err)
	}

	// Static file routes for the subscription service are always enabled.
	// The original path should have had a prefix, but keep this for compatibility.
	router.StaticFile("/all.yaml", saver.OutputPath+"/all.yaml")
	router.StaticFile("/all.txt", saver.OutputPath+"/all.txt")
	router.StaticFile("/base64.txt", saver.OutputPath+"/base64.txt")
	router.StaticFile("/mihomo.yaml", saver.OutputPath+"/mihomo.yaml")
	router.StaticFile("/ACL4SSR_Online_Full.yaml", saver.OutputPath+"/ACL4SSR_Online_Full.yaml")
	// Legacy pudding route used by CM.
	router.StaticFile("/bdg.yaml", saver.OutputPath+"/bdg.yaml")

	router.GET("/sub/*filepath", serveSubFile(saver.OutputPath))

	// pprof routes do not consume performance while idle.
	pprof.Register(router)

	// Enable the web control panel based on config.
	if config.GlobalConfig.EnableWebUI {
		if config.GlobalConfig.APIKey == "" {
			if apiKey := os.Getenv("API_KEY"); apiKey != "" {
				config.GlobalConfig.APIKey = apiKey
			} else {
				config.GlobalConfig.APIKey = GenerateSimpleKey()
				slog.Warn("api-key is not set; generated a random api-key", "api-key", config.GlobalConfig.APIKey)
			}
		}
		slog.Info("Web control panel enabled", "path", "http://ip:port/admin", "api-key", config.GlobalConfig.APIKey)

		// Load templates only when the web control panel is enabled.
		router.SetHTMLTemplate(template.Must(template.New("").ParseFS(configFS, "templates/*.html")))

		// API routes.
		api := router.Group("/api")
		api.Use(app.authMiddleware(config.GlobalConfig.APIKey)) // Add authentication middleware.
		{
			// Config APIs.
			api.GET("/config", app.getConfig)
			api.POST("/config", app.updateConfig)

			// Status APIs.
			api.GET("/status", app.getStatus)
			api.POST("/trigger-check", app.triggerCheckHandler)
			api.POST("/force-close", app.forceCloseHandler)
			// Version APIs.
			api.GET("/version", app.getVersion)

			// Log APIs.
			api.GET("/logs", app.getLogs)
		}

		// Config page.
		router.GET("/admin", func(c *gin.Context) {
			c.HTML(http.StatusOK, "admin.html", gin.H{
				"configPath": app.configPath,
			})
		})
	} else {
		slog.Info("Web control panel disabled")
	}

	// Start HTTP server.
	go func() {
		for {
			if err := router.Run(config.GlobalConfig.ListenPort); err != nil {
				slog.Error(fmt.Sprintf("HTTP server failed to start; restarting: %v", err))
			}
			time.Sleep(30 * time.Second)
		}
	}()
	slog.Info("HTTP server started", "port", config.GlobalConfig.ListenPort)
	return nil
}

func serveSubFile(outputPath string) gin.HandlerFunc {
	return func(c *gin.Context) {
		name := strings.TrimPrefix(c.Param("filepath"), "/")
		name = filepath.Clean(name)
		if name == "." || strings.HasPrefix(name, "..") || filepath.IsAbs(name) {
			c.Status(http.StatusBadRequest)
			return
		}

		path := filepath.Join(outputPath, name)
		typeQuery := c.Query("type")
		if typeQuery == "" {
			c.File(path)
			return
		}
		if !strings.HasSuffix(name, ".yaml") && !strings.HasSuffix(name, ".yml") {
			c.Status(http.StatusBadRequest)
			return
		}

		data, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				c.Status(http.StatusNotFound)
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to read subscription file: %v", err)})
			return
		}

		filtered, err := filterSubscriptionByType(data, parseTypeQuery(typeQuery))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.Data(http.StatusOK, "application/x-yaml; charset=utf-8", filtered)
	}
}

func parseTypeQuery(value string) map[string]struct{} {
	types := make(map[string]struct{})
	for _, part := range strings.Split(value, ",") {
		t := strings.ToLower(strings.TrimSpace(part))
		if t == "" {
			continue
		}
		types[t] = struct{}{}
		switch t {
		case "hy2":
			types["hysteria2"] = struct{}{}
		case "hysteria2":
			types["hy2"] = struct{}{}
		case "socks":
			types["socks5"] = struct{}{}
		}
	}
	return types
}

func filterSubscriptionByType(data []byte, types map[string]struct{}) ([]byte, error) {
	if len(types) == 0 {
		return data, nil
	}

	var doc map[string]any
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("failed to parse subscription YAML: %w", err)
	}

	proxiesRaw, ok := doc["proxies"].([]any)
	if !ok {
		return nil, fmt.Errorf("subscription has no proxies list")
	}

	originalNames := make(map[string]struct{}, len(proxiesRaw))
	filteredNames := make(map[string]struct{}, len(proxiesRaw))
	filtered := make([]any, 0, len(proxiesRaw))
	for _, item := range proxiesRaw {
		proxy, ok := item.(map[string]any)
		if !ok {
			continue
		}
		name, _ := proxy["name"].(string)
		if name != "" {
			originalNames[name] = struct{}{}
		}
		t, _ := proxy["type"].(string)
		if _, ok := types[strings.ToLower(strings.TrimSpace(t))]; !ok {
			continue
		}
		filtered = append(filtered, item)
		if name != "" {
			filteredNames[name] = struct{}{}
		}
	}
	doc["proxies"] = filtered
	pruneProxyGroups(doc, originalNames, filteredNames)

	return yaml.Marshal(doc)
}

func pruneProxyGroups(doc map[string]any, originalNames, filteredNames map[string]struct{}) {
	groups, ok := doc["proxy-groups"].([]any)
	if !ok {
		return
	}
	for _, item := range groups {
		group, ok := item.(map[string]any)
		if !ok {
			continue
		}
		values, ok := group["proxies"].([]any)
		if !ok {
			continue
		}
		pruned := make([]any, 0, len(values))
		for _, value := range values {
			name, ok := value.(string)
			if !ok {
				pruned = append(pruned, value)
				continue
			}
			if _, wasProxy := originalNames[name]; !wasProxy {
				pruned = append(pruned, value)
				continue
			}
			if _, kept := filteredNames[name]; kept {
				pruned = append(pruned, value)
			}
		}
		group["proxies"] = pruned
	}
}

// authMiddleware is API authentication middleware.
func (app *App) authMiddleware(key string) gin.HandlerFunc {
	return func(c *gin.Context) {
		apiKey := c.GetHeader("X-API-Key")
		if subtle.ConstantTimeCompare([]byte(apiKey), []byte(key)) != 1 {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid API key"})
			return
		}
		c.Next()
	}
}

// getConfig returns config file content.
func (app *App) getConfig(c *gin.Context) {
	configData, err := os.ReadFile(app.configPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to read config file: %v", err)})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"content": string(configData),
	})
}

// updateConfig updates config file content.
func (app *App) updateConfig(c *gin.Context) {
	var req struct {
		Content string `json:"content"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request format"})
		return
	}
	// Validate YAML format.
	var yamlData map[string]any
	if err := yaml.Unmarshal([]byte(req.Content), &yamlData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("YAML format error: %v", err)})
		return
	}

	// Write new config.
	if err := os.WriteFile(app.configPath, []byte(req.Content), 0644); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to save config file: %v", err)})
		return
	}

	// Config file watcher reloads config automatically.
	c.JSON(http.StatusOK, gin.H{"message": "config updated"})
}

// getStatus returns application status.
func (app *App) getStatus(c *gin.Context) {
	phaseResults := make(map[string]*check.PhaseResult, 3)
	for i := 1; i <= 3; i++ {
		phaseResults[fmt.Sprintf("%d", i)] = check.GetPhaseResult(i)
	}
	// Pipeline stages run concurrently, so a single `phase` value is no
	// longer expressive enough. Emit a flat pipeline snapshot alongside
	// the legacy fields; the admin UI renders from `pipeline` when present
	// and falls back to `phase` / `progress` / `available` otherwise.
	pipeline := gin.H{
		"total":      check.ProxyCount.Load(),
		"aliveDone":  check.Progress.Load(),
		"alivePass":  check.Available.Load(),
		"mediaDone":  check.MediaDone.Load(),
		"filterPass": check.FilterPassed.Load(),
		"speedDone":  check.SpeedDone.Load(),
		"speedPass":  check.SpeedOk.Load(),
	}
	c.JSON(http.StatusOK, gin.H{
		"checking":     app.checking.Load(),
		"proxyCount":   check.ProxyCount.Load(),
		"available":    check.Available.Load(),
		"progress":     check.Progress.Load(),
		"phase":        check.Phase.Load(),
		"phaseResults": phaseResults,
		"pipeline":     pipeline,
		"hasSpeedTest": config.GlobalConfig.SpeedTestUrl != "",
	})
}

// triggerCheckHandler manually triggers a check.
func (app *App) triggerCheckHandler(c *gin.Context) {
	app.TriggerCheck()
	c.JSON(http.StatusOK, gin.H{"message": "check triggered"})
}

// forceCloseHandler requests a forced stop.
func (app *App) forceCloseHandler(c *gin.Context) {
	check.RequestCancel()
	c.JSON(http.StatusOK, gin.H{"message": "forced stop requested"})
}

// getLogs returns recent logs.
func (app *App) getLogs(c *gin.Context) {
	// Simple implementation: read the last N lines from the log file.
	logPath := TempLog()

	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		c.JSON(http.StatusOK, gin.H{"logs": []string{}})
		return
	}
	lines, err := ReadLastNLines(logPath, 100)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to read logs: %v", err)})
		return
	}
	c.JSON(http.StatusOK, gin.H{"logs": lines})
}

// getVersion returns the application version.
func (app *App) getVersion(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"version": app.version})
}

// ReadLastNLines returns up to n trailing lines of filePath in file order.
// Reads the file backwards in chunks so the scan cost is O(n) instead of
// O(file size) — important because lumberjack lets the log reach 10MB and
// the admin UI polls /api/logs every 10 seconds.
func ReadLastNLines(filePath string, n int) ([]string, error) {
	if n <= 0 {
		return nil, nil
	}
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	if size == 0 {
		return nil, nil
	}

	// Walk backwards, stopping once we have strictly more than n newlines
	// in hand: that guarantees the buffer contains all of the last n
	// complete lines plus at least one partial/boundary line before them,
	// which the final slice discards.
	const chunkSize int64 = 8192
	var buf []byte
	off := size
	for off > 0 {
		readSize := chunkSize
		if off < readSize {
			readSize = off
		}
		off -= readSize

		tmp := make([]byte, readSize)
		if _, err := f.ReadAt(tmp, off); err != nil && err != io.EOF {
			return nil, err
		}
		buf = append(tmp, buf...)

		if int64(bytes.Count(buf, []byte{'\n'})) > int64(n) {
			break
		}
	}

	// Drop trailing newline(s) so Split doesn't produce a spurious empty
	// last element (logs always terminate lines with \n).
	buf = bytes.TrimRight(buf, "\n")
	if len(buf) == 0 {
		return nil, nil
	}

	lines := strings.Split(string(buf), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	// Strip CR for CRLF-terminated logs (bufio.Scanner did this implicitly).
	for i, line := range lines {
		lines[i] = strings.TrimRight(line, "\r")
	}
	return lines, nil
}

func GenerateSimpleKey() string {
	return fmt.Sprintf("%06d", time.Now().UnixNano()%1000000)
}
