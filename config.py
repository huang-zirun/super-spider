"""
incoPat 爬虫配置文件
请在此处设置你的登录凭据
"""

# 登录配置
USERNAME = "8613392011347"  
PASSWORD = "Nz*Ka@Dv" 

# 网站配置
BASE_URL = "https://t.incopat.com/"
LOGIN_URL = "https://t.incopat.com/"

# 浏览器配置
HEADLESS = False    
IMPLICIT_WAIT = 10  
PAGE_LOAD_TIMEOUT = 30      

# 请求配置
REQUEST_DELAY = 2   
MAX_RETRY = 3   

# 文件路径配置
INPUT_FILE = "input_patents.txt"    
OUTPUT_DIR = "output"   
CITING_FILE = "citing_patents.csv"  
CITED_FILE = "cited_patents.csv"  

# 日志配置
LOG_FILE = "crawler.log"
LOG_LEVEL = "INFO"
