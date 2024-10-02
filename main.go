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

	"github.com/PuerkitoBio/goquery"
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
	CleanHTML    bool   `yaml:"clean_html,omitempty"` // 新しいフィールド
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

	log.Printf("Debug: Loaded configuration: %+v", config)
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

func cleanHTMLContent(content string) string {
	doc, err := html.Parse(strings.NewReader(content))
	if err != nil {
		// エラーが発生した場合は元のコンテンツを返す
		return content
	}

	var buf strings.Builder
	var f func(*html.Node)
	f = func(n *html.Node) {
		if n.Type == html.TextNode {
			buf.WriteString(n.Data)
		} else if n.Type == html.ElementNode && n.Data == "br" {
			buf.WriteString("<br>")
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}
	f(doc)

	// 連続する空白を1つにまとめる
	result := regexp.MustCompile(`\s+`).ReplaceAllString(buf.String(), " ")
	return strings.TrimSpace(result)
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

	var jsonData struct {
		Value []map[string]interface{} `json:"value"`
	}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		return nil, fmt.Errorf("ファイル %s のJSONパース中にエラーが発生しました: %v", filePath, err)
	}

	var rows [][]string
	for _, item := range jsonData.Value {
		row, err := processJSONItem(item, profile)
		if err != nil {
			return nil, fmt.Errorf("JSONアイテムの処理中にエラーが発生しました: %v", err)
		}
		if len(row) > 0 {
			rows = append(rows, row)
		}
	}

	return rows, nil
}

func processJSONItem(item map[string]interface{}, profile Profile) ([]string, error) {
	row := make([]string, len(profile.Columns))

	// HTMLコンテンツを含む可能性のあるフィールドを抽出
	var htmlContent string
	if profile.ParseBody != "" {
		content, err := getNestedValue(item, strings.Split(profile.ParseBody, "."))
		if err != nil {
			return nil, fmt.Errorf("HTMLコンテンツの取得中にエラーが発生しました: %v", err)
		}
		htmlContent = content
	}

	htmlValues, err := extractHTMLValues(htmlContent, profile.Columns)
	if err != nil {
		return nil, fmt.Errorf("HTML値の抽出中にエラーが発生しました: %v", err)
	}

	for i, column := range profile.Columns {
		var value string
		if column.Regex != "" {
			// HTML内の値を取得
			value = htmlValues[column.Column]
		} else if column.Key != "" {
			// JSONのトップレベルの値を取得
			var err error
			value, err = getNestedValue(item, strings.Split(column.Key, "."))
			if err != nil {
				log.Printf("Warning: ネストされた値の取得中にエラーが発生しました: %v", err)
			}
		}

		// HTMLのクリーニングが必要な場合
		if column.CleanHTML {
			value = cleanHTMLContent(value)
		}

		row[i] = handleNewlines(value)
		row[i] = handleNullValue(row[i])
	}

	// デバッグログを1回だけ出力
	for i, column := range profile.Columns {
		log.Printf("Debug: Column %s, Value: %s", column.Column, row[i])
	}

	return row, nil
}

func extractHTMLValues(content string, columns []Column) (map[string]string, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(content))
	if err != nil {
		return nil, fmt.Errorf("HTMLの解析中にエラーが発生しました: %v", err)
	}

	results := make(map[string]string)

	doc.Find("p").Each(func(i int, s *goquery.Selection) {
		var currentKey string
		var currentValue strings.Builder

		s.Contents().Each(func(j int, node *goquery.Selection) {
			if goquery.NodeName(node) == "strong" {
				if currentKey != "" {
					value := strings.TrimSpace(currentValue.String())
					for _, column := range columns {
						if column.Regex != "" && strings.Contains(currentKey, column.Regex) {
							results[column.Column] = value
							break
						}
					}
					currentValue.Reset()
				}
				currentKey = strings.TrimSuffix(strings.TrimSpace(node.Text()), ":")
			} else {
				currentValue.WriteString(node.Text())
			}
		})

		// 最後のキーと値のペアを保存
		if currentKey != "" {
			value := strings.TrimSpace(currentValue.String())
			for _, column := range columns {
				if column.Regex != "" && strings.Contains(currentKey, column.Regex) {
					results[column.Column] = value
					break
				}
			}
		}
	})

	log.Printf("Debug: Extracted HTML Values: %v", results) // デバッグログ
	return results, nil
}

func cleanValue(value string) string {
	value = strings.ReplaceAll(value, "\u00A0", " ")               // &nbsp; を通常の空白に置換
	value = regexp.MustCompile(`\s+`).ReplaceAllString(value, " ") // 連続する空白を1つに
	return strings.TrimSpace(value)                                // 先頭と末尾の空白を削除
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
