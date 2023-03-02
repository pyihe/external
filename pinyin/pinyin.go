package pinyin

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

type (
	Style   uint8 // 输出风格
	vowel   int32
	Options func(*Adapter)
)

type Adapter struct {
	tempPath   string           // 拼音模板路径
	toneTemp   map[vowel]vowel  // 元音模板
	pinYinTemp map[vowel]string // 从tempPath中读取出来的拼音模板
}

const (
	None = "9999"
)

const (
	Normal           = 0 // 普通风格：全小写并且不带声调
	Tone             = 1 // 全小写，带声调
	InitialBigLetter = 2 // 首字母大写不带声调
)

func WithTempPath(path string) Options {
	return func(adapter *Adapter) {
		adapter.tempPath = path
	}
}

func NewAdapter(opts ...Options) *Adapter {
	var adp = &Adapter{
		toneTemp:   make(map[vowel]vowel),
		pinYinTemp: make(map[vowel]string),
	}

	for _, opt := range opts {
		opt(adp)
	}

	// 加载拼音配置文件，初始化模板
	adp.init()

	return adp
}

// 初始化
func (adp *Adapter) init() {
	// 初始化拼音模板
	if adp.tempPath != "" {
		tempFile, err := os.Open(adp.tempPath)
		if err != nil {
			panic(err)
		}
		defer tempFile.Close()

		scanner := bufio.NewScanner(tempFile)
		for scanner.Scan() {
			row := strings.Split(scanner.Text(), "=>")
			if len(row) < 2 {
				continue
			}
			temp, err := strconv.ParseInt(row[0], 16, 32)
			if err != nil {
				continue
			}
			adp.pinYinTemp[vowel(temp)] = row[1]
		}
	} else {
		for k, v := range pinyinDict {
			adp.pinYinTemp[vowel(k)] = v
		}
	}

	// 初始化元音模板
	var (
		firstTone  = []vowel{'ā', 'ē', 'ī', 'ō', 'ū', 'ǖ', 'Ā', 'Ē', 'Ī', 'Ō', 'Ū', 'Ǖ'} // 单韵母 一声
		secondTone = []vowel{'á', 'é', 'í', 'ó', 'ú', 'ǘ', 'Á', 'É', 'Í', 'Ó', 'Ú', 'Ǘ'} // 单韵母 二声
		thirdTone  = []vowel{'ǎ', 'ě', 'ǐ', 'ǒ', 'ǔ', 'ǚ', 'Ǎ', 'Ě', 'Ǐ', 'Ǒ', 'Ǔ', 'Ǚ'} // 单韵母 三声
		fourthTone = []vowel{'à', 'è', 'ì', 'ò', 'ù', 'ǜ', 'À', 'È', 'Ì', 'Ò', 'Ù', 'Ǜ'} // 单韵母 四声
		noTone     = []vowel{'a', 'e', 'i', 'o', 'u', 'v', 'A', 'E', 'I', 'O', 'U', 'V'} // 单韵母 无声调
	)

	for i, e := range firstTone {
		adp.toneTemp[e] = noTone[i]
	}
	for i, e := range secondTone {
		adp.toneTemp[e] = noTone[i]
	}
	for i, e := range thirdTone {
		adp.toneTemp[e] = noTone[i]
	}
	for i, e := range fourthTone {
		adp.toneTemp[e] = noTone[i]
	}
}

// ParseHans 解析汉字
func (adp *Adapter) ParseHans(hans, split string, yinType Style) string {
	var originalHans = []vowel(hans)
	var words []string

	for _, single := range originalHans {
		if unicode.Is(unicode.Han, rune(single)) == false {
			words = append(words, string(single))
			continue
		}
		word := adp.parseSingleHan(single, yinType)
		if len(word) > 0 {
			words = append(words, word)
		}
	}
	return strings.Join(words, split)
}

// ParseSingleHan 解析单个汉字
func (adp *Adapter) parseSingleHan(han vowel, yinType Style) (result string) {
	switch yinType {
	case Tone:
		result = adp.tone(han)
	case InitialBigLetter:
		result = adp.initialWithBigLetter(han)
	default:
		result = adp.defaultTone(han)
	}
	return
}

// 带声调
func (adp *Adapter) tone(han vowel) string {
	data, ok := adp.pinYinTemp[han]
	if ok && data != "" {
		return strings.Split(data, ",")[0]
	}
	return None
}

// 首字母大写不带声调
func (adp *Adapter) initialWithBigLetter(han vowel) (result string) {
	def := adp.defaultTone(han)
	if def == "" {
		return def
	}
	str := []vowel(def)
	if str[0] > 32 {
		str[0] = str[0] - 32
	}
	for _, v := range str {
		result += string(v)
	}
	return
}

func (adp *Adapter) defaultTone(han vowel) (result string) {
	tone := adp.tone(han)
	if tone == "" {
		return None
	}

	var resultVowel = make([]vowel, utf8.RuneCountInString(tone))
	var count = 0
	for _, t := range tone {
		data, ok := adp.toneTemp[vowel(t)]
		if ok {
			resultVowel[count] = data
		} else {
			resultVowel[count] = vowel(t)
		}
		count++
	}
	for _, e := range resultVowel {
		result += string(e)
	}
	return
}
