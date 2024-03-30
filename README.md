> **免责声明：**

>本仓库的所有内容仅供学习和参考之用，禁止用于商业用途。任何人或组织不得将本仓库的内容用于非法用途或侵犯他人合法权益。本仓库所涉及的爬虫技术仅用于学习和研究，不得用于对其他平台进行大规模爬虫或其他非法行为。对于因使用本仓库内容而引起的任何法律责任，本仓库不承担任何责任。使用本仓库的内容即表示您同意本免责声明的所有条款和条件。

# 仓库描述

**B站弹幕爬虫**
目前能抓取B站弹幕信息。

原理：利用[playwright](https://playwright.dev/)搭桥，保留登录成功后的上下文浏览器环境，通过执行JS表达式获取一些加密参数
通过使用此方式，免去了复现核心加密JS代码，逆向难度大大降低  

## 使用方法

### 安装 playwright浏览器驱动

   ```shell
   playwright install
   ```

### 运行爬虫程序

   ```shell

   # 从配置文件中读取指定的视频ID列表获取指定视频的弹幕
   python spider.py --lt qrcode
  
   # 打开对应APP扫二维码登录
     
   # 其他平台爬虫使用示例, 执行下面的命令查看
   python spider.py --help    
   ```


### 数据保存
- 支持保存到csv中（data/目录下）

## 运行报错常见问题Q&A
> 遇到问题先自行搜索解决下，现在AI很火，用ChatGPT大多情况下能解决你的问题

## 参考

- relakkes MediaCrawler(https://github.com/NanmiCoder/MediaCrawler)
