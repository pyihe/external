### Usage
```go
package main

import (
    "fmt"

    pinyin "github.com/pyihe/go-pinyin"
)

func main() {
    adp := pinyin.NewAdapter()
    fmt.Println(adp.ParseHans("我爱中国.", "", pinyin.Normal))           // output: woaizhongguo.
    fmt.Println(adp.ParseHans("我爱中国.", "", pinyin.Tone))             // output: wǒàizhōngguó.
    fmt.Println(adp.ParseHans("我爱中国.", "", pinyin.InitialBigLetter)) // output: WoAiZhongGuo.
}
```