package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"golang.org/x/net/html"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
	"gopkg.in/yaml.v2"
)

// Config struct
type Config struct {
	Profiles           map[string]Profile `yaml:"profiles"`
	NewlineHandling    string             `yaml:"newline_handling"`
	NewlineReplacement string             `yaml:"newline_replacement"`
	NullValueHandling  string             `yaml:"null_value_handling"`
}

type Profile struct {
	Columns     []Column `yaml:"columns"`
	OutputFile  string   `yaml:"output_file"`
	ParseBody   string   `yaml:"parse_body"`
	ContentType string   `yaml:"content_type"`
}

type Column struct {
	Key          string `yaml:"key"`
	Column       string `yaml:"column"`
	Regex        string `yaml:"regex,omitempty"`
	ExtractToEnd bool   `yaml:"extract_to_end,omitempty"`
	Format       string `yaml:"format,omitempty"`
}

var (
	inputFolder  string
	configFile   = "config.yaml"
	profileName  string
	workerCount  int
	config       Config
	progressChan chan int
	totalFiles   int
	errorChan    chan error
)

func init() {
	flag.StringVar(&profileName, "p", "", "使用するプロファイル名")
	flag.IntVar(&workerCount, "w", runtime.NumCPU(), "並行処理のワーカー数")
	flag.Parse()

	if flag.NArg() < 1 {
		log.Fatal("使用方法: go run main.go [flags] <JSONファイルのフォルダパス>")
	}
	inputFolder = flag.Arg(0)

	// 設定ファイルの読み込み
	data, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatalf("設定ファイル %s の読み込みエラー: %v", configFile, err)
	}

	if err := yaml.Unmarshal(data, &config); err != nil {
		log.Fatalf("設定ファイルの解析エラー: %v", err)
	}

	if profileName == "" {
		log.Fatal("プロファイル名を指定してください (-p フラグを使用)")
	}

	if _, ok := config.Profiles[profileName]; !ok {
		log.Fatalf("指定されたプロファイル '%s' が見つかりません", profileName)
	}
}

func main() {
	jsonFiles, err := filepath.Glob(filepath.Join(inputFolder, "*.json"))
	if err != nil {
		log.Fatalf("JSONファイルの検索中にエラーが発生しました: %v", err)
	}
	totalFiles = len(jsonFiles)

	profile := config.Profiles[profileName]
	outputFile, err := os.Create(profile.OutputFile)
	if err != nil {
		log.Fatalf("CSVファイルの作成中にエラーが発生しました: %v", err)
	}
	defer outputFile.Close()

	writer := csv.NewWriter(outputFile)
	defer writer.Flush()

	// ヘッダー行を書き込む
	headers := getHeaders(profile)
	if err := writer.Write(headers); err != nil {
		log.Fatalf("CSVヘッダーの書き込み中にエラーが発生しました: %v", err)
	}

	// 並行処理の準備
	jobs := make(chan string, totalFiles)
	results := make(chan []string, totalFiles)
	progressChan = make(chan int, totalFiles)
	errorChan = make(chan error, totalFiles)

	var wg sync.WaitGroup
	// ワーカーの起動
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker(jobs, results, &wg, profile)
	}

	// プログレス表示の開始
	go showProgress()

	// エラー監視の開始
	go monitorErrors()

	// ジョブの送信
	for _, file := range jsonFiles {
		jobs <- file
	}
	close(jobs)

	// 全てのワーカーの終了を待つ
	go func() {
		wg.Wait()
		close(results)
		close(errorChan)
	}()

	// 結果の収集と書き込み
	for row := range results {
		if err := writer.Write(row); err != nil {
			log.Printf("CSVへの書き込み中にエラーが発生しました: %v", err)
		}
		select {
		case progressChan <- 1:
		default:
			// チャネルが一杯の場合は進捗更新をスキップ
		}
	}

	close(progressChan)

	// 進捗表示の終了を待つ
	time.Sleep(100 * time.Millisecond)

	fmt.Println("\n処理が完了しました。出力ファイル: ", outputFile.Name())
}

func getHeaders(profile Profile) []string {
	headers := make([]string, len(profile.Columns))
	for i, column := range profile.Columns {
		headers[i] = column.Column
	}
	return headers
}

func worker(jobs <-chan string, results chan<- []string, wg *sync.WaitGroup, profile Profile) {
	defer wg.Done()
	for filePath := range jobs {
		rows, err := processJSONFile(filePath, profile)
		if err != nil {
			errorChan <- fmt.Errorf("ファイル %s の処理中にエラーが発生しました: %v", filePath, err)
			continue
		}
		for _, row := range rows {
			results <- row
		}
	}
}

func processJSONFile(filePath string, profile Profile) ([][]string, error) {
	data, err := decodeFileContent(filePath)
	if err != nil {
		return nil, fmt.Errorf("ファイル %s の読み込み中にエラーが発生しました: %v", filePath, err)
	}

	var jsonData map[string]interface{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		return nil, fmt.Errorf("ファイル %s のJSONパース中にエラーが発生しました: %v", filePath, err)
	}

	var rows [][]string
	if valueArray, ok := jsonData["value"].([]interface{}); ok {
		for _, item := range valueArray {
			row, err := processJSONItem(item, profile)
			if err != nil {
				return nil, fmt.Errorf("JSONアイテムの処理中にエラーが発生しました: %v", err)
			}
			if len(row) > 0 {
				rows = append(rows, row)
			}
		}
	}

	return rows, nil
}
func processJSONItem(item interface{}, profile Profile) ([]string, error) {
	row := make([]string, len(profile.Columns))
	parseContent := ""
	if profile.ParseBody != "" {
		var err error
		parseContent, err = getNestedValue(item, strings.Split(profile.ParseBody, "."))
		if err != nil {
			return nil, fmt.Errorf("ネストされた値の取得中にエラーが発生しました: %v", err)
		}
		log.Printf("Debug: ParseContent: %s", parseContent) // デバッグログ

		if profile.ContentType == "html" {
			parseContent = cleanContent(parseContent)
			log.Printf("Debug: Cleaned ParseContent: %s", parseContent) // デバッグログ
		}
	}

	// HTML形式の列のキーワードを収集
	htmlKeywords := []string{}
	for _, column := range profile.Columns {
		if column.Regex != "" {
			htmlKeywords = append(htmlKeywords, column.Regex)
		}
	}

	// HTML値を一度に抽出
	htmlValues, err := extractHTMLValues(parseContent, htmlKeywords)
	if err != nil {
		return nil, fmt.Errorf("HTML値の抽出中にエラーが発生しました: %v", err)
	}
	log.Printf("Debug: Extracted HTML Values: %v", htmlValues) // デバッグログ

	for i, column := range profile.Columns {
		var value string
		if column.Regex != "" {
			value = htmlValues[column.Regex]
		} else if column.Key != "" {
			var err error
			value, err = getNestedValue(item, strings.Split(column.Key, "."))
			if err != nil {
				return nil, fmt.Errorf("ネストされた値の取得中にエラーが発生しました: %v", err)
			}
		}
		log.Printf("Debug: Column %s, Value: %s", column.Column, value) // デバッグログ
		row[i] = handleNewlines(value)
		row[i] = handleNullValue(row[i])
	}

	// 空行のスキップ（全ての値が空または指定されたnull値の場合もスキップ）
	if isEmptyOrAllNull(row) {
		return nil, nil
	}

	return row, nil
}

// 関連する補助関数

func getNestedValue(data interface{}, keys []string) (string, error) {
	for _, key := range keys {
		switch v := data.(type) {
		case map[string]interface{}:
			var ok bool
			data, ok = v[key]
			if !ok {
				return "", fmt.Errorf("キー '%s' が見つかりません", key)
			}
		case []interface{}:
			index := 0
			_, err := fmt.Sscanf(key, "[%d]", &index)
			if err != nil {
				return "", fmt.Errorf("配列インデックスの解析中にエラーが発生しました: %v", err)
			}
			if index >= 0 && index < len(v) {
				data = v[index]
			} else {
				return "", fmt.Errorf("インデックス %d が配列の範囲外です", index)
			}
		default:
			return fmt.Sprintf("%v", data), nil
		}
	}
	return fmt.Sprintf("%v", data), nil
}

func cleanContent(input string) string {
	if input == "" {
		return ""
	}

	// Remove the content prefix and suffix
	input = strings.TrimPrefix(input, "@{contentType=html; content=")
	input = strings.TrimSuffix(input, "}")

	// Remove HTML tags
	doc, err := html.Parse(strings.NewReader(input))
	if err != nil {
		log.Printf("HTMLの解析中にエラーが発生しました: %v", err)
		return input
	}
	var textContent string
	var extractText func(*html.Node)
	extractText = func(n *html.Node) {
		if n.Type == html.TextNode {
			textContent += n.Data
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			extractText(c)
		}
	}
	extractText(doc)

	// 余分な空白を削除（ただし、単語間の空白は保持）
	textContent = strings.TrimSpace(textContent)
	space := regexp.MustCompile(`\s{2,}`)
	textContent = space.ReplaceAllString(textContent, " ")

	// 特殊文字のエスケープを解除
	textContent = html.UnescapeString(textContent)

	// 残っているかもしれない @{contentType=html; content= を削除
	contentTypeRegex := regexp.MustCompile(`@\{contentType=html; content=.*?\}`)
	textContent = contentTypeRegex.ReplaceAllString(textContent, "")

	return textContent
}

func handleNewlines(input string) string {
	switch config.NewlineHandling {
	case "keep":
		return input
	case "remove":
		return strings.ReplaceAll(input, "\r\n", "")
	case "replace":
		return strings.ReplaceAll(input, "\r\n", config.NewlineReplacement)
	default:
		return input
	}
}

func handleNullValue(value string) string {
	if value == "" {
		switch config.NullValueHandling {
		case "null":
			return "null"
		case "nil":
			return "nil"
		case "empty":
			return ""
		default:
			return ""
		}
	}
	return value
}

func isEmptyOrAllNull(row []string) bool {
	nullValue := handleNullValue("")
	for _, value := range row {
		if value != "" && value != nullValue {
			return false
		}
	}
	return true
}

func extractHTMLValues(content string, keywords []string) (map[string]string, error) {
	doc, err := html.Parse(strings.NewReader(content))
	if err != nil {
		return nil, fmt.Errorf("HTMLの解析中にエラーが発生しました: %v", err)
	}

	results := make(map[string]string)
	var currentKey string
	var buffer strings.Builder
	var extractFunc func(*html.Node)

	extractFunc = func(n *html.Node) {
		if n.Type == html.ElementNode && (n.Data == "strong" || n.Data == "b") {
			if n.FirstChild != nil && n.FirstChild.Type == html.TextNode {
				for _, keyword := range keywords {
					if strings.HasPrefix(n.FirstChild.Data, keyword) {
						if currentKey != "" {
							results[currentKey] = strings.TrimSpace(buffer.String())
							buffer.Reset()
						}
						currentKey = keyword
						break
					}
				}
			}
		} else if currentKey != "" && n.Type == html.TextNode {
			buffer.WriteString(n.Data)
		}

		for c := n.FirstChild; c != nil; c = c.NextSibling {
			extractFunc(c)
		}

		if n.Type == html.ElementNode && n.Data == "p" {
			if currentKey != "" {
				results[currentKey] = strings.TrimSpace(buffer.String())
				buffer.Reset()
				currentKey = ""
			}
		}
	}

	extractFunc(doc)

	if currentKey != "" {
		results[currentKey] = strings.TrimSpace(buffer.String())
	}

	// 結果をクリーンアップ
	for k, v := range results {
		v = strings.ReplaceAll(v, "\u00A0", " ")               // &nbsp; を通常の空白に置換
		v = regexp.MustCompile(`\s+`).ReplaceAllString(v, " ") // 連続する空白を1つに
		v = strings.TrimSpace(v)                               // 先頭と末尾の空白を削除
		v = strings.TrimPrefix(v, k)                           // キーワードを削除
		v = strings.TrimSpace(v)                               // 再度、先頭と末尾の空白を削除
		results[k] = v
	}

	log.Printf("Debug: Extracted HTML Values: %v", results) // デバッグログ
	return results, nil
}

func extractValue(content, key string, extractToEnd bool) (string, error) {
	lines := strings.Split(content, "\r\n")
	pattern := regexp.QuoteMeta(key) + `(.+)`
	re, err := regexp.Compile(pattern)
	if err != nil {
		return "", fmt.Errorf("正規表現のコンパイル中にエラーが発生しました: %v", err)
	}

	for i, line := range lines {
		matches := re.FindStringSubmatch(line)
		if len(matches) > 1 {
			if extractToEnd {
				// キーワードが見つかった行から、キーワード以降の部分を抽出
				keywordIndex := strings.Index(line, key)
				if keywordIndex != -1 {
					restOfLine := line[keywordIndex+len(key):]
					extractedLines := append([]string{restOfLine}, lines[i+1:]...)
					return strings.TrimSpace(strings.Join(extractedLines, "\r\n")), nil
				}
			} else {
				value := strings.TrimSpace(matches[1])
				// 次の行にキーワードがあるかチェック
				if i+1 < len(lines) && strings.Contains(lines[i+1], ":") {
					return "", nil // null_value_handling に基づいた値を設定
				}
				return value, nil
			}
		}
	}
	return "", nil
}

func getNestedValue(data interface{}, keys []string) (string, error) {
	for _, key := range keys {
		switch v := data.(type) {
		case map[string]interface{}:
			var ok bool
			data, ok = v[key]
			if !ok {
				return "", fmt.Errorf("キー '%s' が見つかりません", key)
			}
		case []interface{}:
			index := 0
			_, err := fmt.Sscanf(key, "[%d]", &index)
			if err != nil {
				return "", fmt.Errorf("配列インデックスの解析中にエラーが発生しました: %v", err)
			}
			if index >= 0 && index < len(v) {
				data = v[index]
			} else {
				return "", fmt.Errorf("インデックス %d が配列の範囲外です", index)
			}
		default:
			return fmt.Sprintf("%v", data), nil
		}
	}
	return fmt.Sprintf("%v", data), nil
}

func decodeFileContent(filePath string) ([]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// まずUTF-8として読み込みを試みる
	content, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	// UTF-8として有効かチェック
	if utf8.Valid(content) {
		return content, nil
	}

	// UTF-8でない場合、UTF-16LEとして再度デコードを試みる
	file.Seek(0, 0) // ファイルポインタを先頭に戻す
	decoder := unicode.UTF16(unicode.LittleEndian, unicode.UseBOM).NewDecoder()
	reader := transform.NewReader(file, decoder)
	decodedContent, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	return decodedContent, nil
}

func cleanContent(input string) string {
	if input == "" {
		return ""
	}

	// Remove the content prefix and suffix
	input = strings.TrimPrefix(input, "@{contentType=html; content=")
	input = strings.TrimSuffix(input, "}")

	// Remove HTML tags
	doc, err := html.Parse(strings.NewReader(input))
	if err != nil {
		log.Printf("HTMLの解析中にエラーが発生しました: %v", err)
		return input
	}
	var textContent string
	var extractText func(*html.Node)
	extractText = func(n *html.Node) {
		if n.Type == html.TextNode {
			textContent += n.Data
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			extractText(c)
		}
	}
	extractText(doc)

	// 余分な空白を削除（ただし、単語間の空白は保持）
	textContent = strings.TrimSpace(textContent)
	space := regexp.MustCompile(`\s{2,}`)
	textContent = space.ReplaceAllString(textContent, " ")

	// 特殊文字のエスケープを解除
	textContent = html.UnescapeString(textContent)

	// 残っているかもしれない @{contentType=html; content= を削除
	contentTypeRegex := regexp.MustCompile(`@\{contentType=html; content=.*?\}`)
	textContent = contentTypeRegex.ReplaceAllString(textContent, "")

	return textContent
}

func handleNewlines(input string) string {
	switch config.NewlineHandling {
	case "keep":
		return input
	case "remove":
		return strings.ReplaceAll(input, "\r\n", "")
	case "replace":
		return strings.ReplaceAll(input, "\r\n", config.NewlineReplacement)
	default:
		return input
	}
}

func handleNullValue(value string) string {
	if value == "" {
		switch config.NullValueHandling {
		case "null":
			return "null"
		case "nil":
			return "nil"
		case "empty":
			return ""
		default:
			return ""
		}
	}
	return value
}

func isEmptyOrAllNull(row []string) bool {
	nullValue := handleNullValue("")
	for _, value := range row {
		if value != "" && value != nullValue {
			return false
		}
	}
	return true
}

func showProgress() {
	processed := 0
	start := time.Now()
	for range progressChan {
		processed++
		if processed > totalFiles {
			processed = totalFiles
		}
		progress := float64(processed) / float64(totalFiles) * 100
		if progress > 100.0 {
			progress = 100.0
		}
		elapsed := time.Since(start)
		var estTotal time.Duration
		if processed > 0 {
			estTotal = elapsed / time.Duration(processed) * time.Duration(totalFiles)
		}
		fmt.Printf("\r進捗: %.2f%% (%d/%d) - 経過時間: %v - 推定残り時間: %v",
			progress, processed, totalFiles, elapsed.Round(time.Second),
			(estTotal - elapsed).Round(time.Second))
	}
	fmt.Println() // 最後に改行を入れて、次の出力が同じ行に表示されないようにする
}

func monitorErrors() {
	for err := range errorChan {
		log.Printf("エラー: %v", err)
	}
}
