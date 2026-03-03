"""
incoPat 专利数据库爬虫 - 使用 DrissionPage
功能：批量爬取专利的引证专利和被引证专利数据，保存为DTA格式
v7版本：
- 从dta文件自动读取专利号
- 添加年份变量
- 分块处理大数据量
- 优化速度
- 不生成中间HTML文件
"""

import os
import time
import logging
import re
import glob
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from DrissionPage import ChromiumPage, ChromiumOptions

import config


# 配置日志
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(config.LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class IncoPatCrawler:
    """incoPat 爬虫主类"""
    
    def __init__(self):
        self.page: Optional[ChromiumPage] = None
        self.is_logged_in = False
        
        # 创建输出目录
        os.makedirs(config.OUTPUT_DIR, exist_ok=True)
        
        # 初始化结果存储
        self.self_citing_data: List[Dict] = []  # 自引数据
        self.other_citing_data: List[Dict] = []  # 他引数据
        
        # 统计信息
        self.processed_count = 0
        self.failed_count = 0
        self.start_time = None
    
    def init_driver(self):
        """初始化浏览器"""
        logger.info("正在初始化浏览器...")
        
        try:
            co = ChromiumOptions()
            
            if config.HEADLESS:
                co.headless(True)
            
            co.set_argument('--no-sandbox')
            co.set_argument('--disable-gpu')
            co.set_argument('--window-size', '1920,1080')
            co.set_user_agent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                            '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            self.page = ChromiumPage(addr_or_opts=co)
            self.page.set.timeouts(base=config.IMPLICIT_WAIT, page_load=config.PAGE_LOAD_TIMEOUT)
            
            logger.info("浏览器初始化成功")
        except Exception as e:
            logger.error(f"浏览器初始化失败: {e}")
            raise
    
    def _dismiss_cookie_banner(self) -> None:
        """关闭页面底部的 Cookie 同意弹窗，避免遮挡登录表单"""
        try:
            # 等待弹窗出现
            time.sleep(1)
            # 优先点击「接受所有 Cookie」按钮
            accept_btn = self.page.ele('xpath://button[contains(text(),"接受所有 Cookie")]', timeout=2)
            if accept_btn:
                accept_btn.click()
                logger.info("已点击「接受所有 Cookie」关闭弹窗")
                time.sleep(0.5)
                return
            # 备选：通过文本匹配（可能在 span 内）
            accept_btn = self.page.ele('xpath://*[contains(text(),"接受所有 Cookie")]', timeout=1)
            if accept_btn:
                accept_btn.click()
                logger.info("已通过文本匹配关闭 Cookie 弹窗")
                time.sleep(0.5)
                return
            # 备选：点击关闭按钮 X（部分站点用 aria-label 或 class）
            close_btn = self.page.ele('xpath://*[contains(@aria-label,"关闭") or contains(@class,"close")][last()]', timeout=1)
            if close_btn:
                close_btn.click()
                time.sleep(0.5)
        except Exception as e:
            logger.debug(f"未检测到 Cookie 弹窗或关闭失败（可忽略）: {e}")
    
    def login(self) -> bool:
        """登录 incoPat 系统"""
        if self.is_logged_in:
            return True
        
        logger.info("正在登录 incoPat...")
        
        try:
            self.page.get(config.LOGIN_URL)
            time.sleep(3)  # 减少等待时间
            self._dismiss_cookie_banner()
            
            # 输入用户名（多种选择器兼容不同页面结构）
            username_selectors = [
                'xpath://input[contains(@placeholder,"用户名")]',
                'xpath://input[contains(@placeholder,"请输入用户名") or contains(@placeholder,"账号")]',
                'xpath://input[@type="text" and (contains(@name,"user") or contains(@name,"login") or contains(@id,"user") or contains(@id,"login") or contains(@id,"account"))]',
                'xpath://label[contains(text(),"用户名")]/following-sibling::*//input | //label[contains(text(),"用户名")]/../input',
                'xpath://*[contains(text(),"用户名")]/following::input[@type="text"][1]',
                'xpath://form//input[@type="text"][1]',
            ]
            username_input = None
            for sel in username_selectors:
                username_input = self.page.ele(sel, timeout=2)
                if username_input:
                    break
            if not username_input:
                logger.error("未找到用户名输入框")
                return False
            
            username_input.clear()
            username_input.input(config.USERNAME)
            time.sleep(0.3)
            
            # 输入密码（多种选择器）
            password_selectors = [
                'xpath://input[contains(@placeholder,"密码")]',
                'xpath://input[@type="password"]',
                'xpath://input[contains(@name,"pass") or contains(@name,"pwd") or contains(@id,"pass") or contains(@id,"pwd")]',
                'xpath://label[contains(text(),"密码")]/following-sibling::*//input | //label[contains(text(),"密码")]/../input',
                'xpath://form//input[@type="password"]',
            ]
            password_input = None
            for sel in password_selectors:
                password_input = self.page.ele(sel, timeout=2)
                if password_input:
                    break
            if not password_input:
                logger.error("未找到密码输入框")
                return False
            
            password_input.clear()
            password_input.input(config.PASSWORD)
            time.sleep(0.3)
            
            # 勾选同意条款
            try:
                is_checked = self.page.run_js('return document.getElementById("clauseCheckBox").checked')
                if not is_checked:
                    self.page.run_js('document.getElementById("clauseCheckBox").click()')
            except:
                pass
            
            time.sleep(0.3)
            
            # 点击登录按钮
            self.page.run_js('document.getElementById("loginBtn").click()')
            
            time.sleep(4)  # 减少等待时间
            
            # 检查登录结果
            current_url = self.page.url
            
            if 'login' in current_url.lower():
                logger.error("登录失败，请检查用户名和密码")
                return False
            
            self.is_logged_in = True
            logger.info("登录成功！")
            return True
                
        except Exception as e:
            logger.error(f"登录过程出错: {e}")
            return False
    
    def search_patent(self, patent_no: str) -> Optional[Dict]:
        """搜索专利并获取puuid"""
        try:
            # 访问搜索页面
            self.page.get('https://t.incopat.com/search')
            time.sleep(2)  # 减少等待时间
            
            # 查找搜索框
            search_input = self.page.ele('xpath://input[@id="searchValue"]', timeout=5)
            if not search_input:
                return None
            
            # 输入专利号并搜索
            search_input.clear()
            search_input.input(patent_no)
            time.sleep(0.5)
            search_input.input('\n')
            time.sleep(3)  # 减少等待时间
            
            # 尝试从页面中提取puuid
            try:
                solr_query_bean = self.page.ele('xpath://input[@id="solrQueryBeanId"]', timeout=3)
                if solr_query_bean:
                    puuid = solr_query_bean.attr('value')
                    return {'puuid': puuid, 'patent_no': patent_no}
            except:
                pass
            
            return None
            
        except Exception as e:
            logger.error(f"搜索专利失败: {e}")
            return None
    
    def build_cite_url(self, patent_info: Dict) -> str:
        """构建引证分析页面URL"""
        puuid = patent_info.get('puuid')
        
        if puuid:
            url = (f"https://t.incopat.com/detail/init?"
                   f"puuid_g={puuid}&"
                   f"order_g=1&"
                   f"rows_g=20&"
                   f"sortFld_g=PD&"
                   f"basc_g=false&"
                   f"secondSortFld_g=&"
                   f"secondBasc_g=false&"
                   f"bdType_g=0&"
                   f"bdOrder_g=asc&"
                   f"currentMainTab_g=baseInfoTab&"
                   f"moduleType=simpleSearch")
            return url
        return None
    
    def extract_citation_data(self, patent_info: Dict, year: int) -> Tuple[List[Dict], List[Dict]]:
        """提取引证数据"""
        self_citing_list = []
        other_citing_list = []
        
        patent_no = patent_info.get('patent_no')
        
        try:
            # 构建引证分析页面URL
            cite_url = self.build_cite_url(patent_info)
            if not cite_url:
                return [], []
            
            self.page.get(cite_url)
            time.sleep(4)  # 减少等待时间
            
            # 提取自引专利
            try:
                self_citing_list = self._extract_patent_data_from_table('selfCiteTable', patent_no, year)
            except Exception as e:
                logger.debug(f"提取自引专利失败: {e}")
            
            # 提取他引专利
            try:
                other_citing_list = self._extract_patent_data_from_table('otherCiteTable', patent_no, year)
            except Exception as e:
                logger.debug(f"提取他引专利失败: {e}")
            
            return self_citing_list, other_citing_list
            
        except Exception as e:
            logger.error(f"提取引证数据失败: {e}")
            return [], []
    
    def _extract_patent_data_from_table(self, table_id: str, source_patent: str, year: int) -> List[Dict]:
        """从指定表格中提取专利数据"""
        patent_list = []
        
        try:
            # 查找指定表格
            table = self.page.ele(f'xpath://table[@id="{table_id}"]', timeout=3)
            if not table:
                return []
            
            # 查找表格行
            rows = table.eles('xpath:.//tbody//tr[@data-index]')
            
            if not rows:
                return []
            
            for row in rows:
                try:
                    cells = row.eles('xpath:.//td')
                    if len(cells) < 4:
                        continue
                    
                    # 提取专利号
                    patent_no = ""
                    try:
                        patent_link = cells[3].ele('xpath:.//a[@data-pn]', timeout=1)
                        if patent_link:
                            patent_no = patent_link.attr('data-pn')
                    except:
                        patent_no = cells[3].text.strip() if cells[3].text else ""
                    
                    # 提取IPC
                    ipc = ""
                    try:
                        ipc_cell = cells[-1]
                        ipc_links = ipc_cell.eles('xpath:.//a[@data-query]')
                        if ipc_links:
                            ipc_list = [link.attr('data-query') for link in ipc_links if link.attr('data-query')]
                            ipc = '; '.join(ipc_list)
                    except:
                        pass
                    
                    # 验证并添加
                    if patent_no and len(patent_no) >= 5:
                        if any(patent_no.startswith(code) for code in ['CN', 'US', 'EP', 'WO', 'JP', 'KR']):
                            patent_list.append({
                                'source_patent': source_patent,
                                'citation_patent': patent_no,
                                'ipc': ipc,
                                'year': year  # 添加年份变量
                            })
                        
                except Exception as e:
                    continue
            
            return patent_list
            
        except Exception as e:
            return []
    
    def process_patent(self, patent_no: str, year: int) -> bool:
        """处理单个专利"""
        try:
            # 搜索专利并获取信息
            patent_info = self.search_patent(patent_no)
            
            if not patent_info:
                self.failed_count += 1
                return False
            
            # 提取引证数据
            self_citing_list, other_citing_list = self.extract_citation_data(patent_info, year)
            
            # 添加到结果
            self.self_citing_data.extend(self_citing_list)
            self.other_citing_data.extend(other_citing_list)
            
            self.processed_count += 1
            
            # 每处理10个专利显示进度
            if self.processed_count % 10 == 0:
                elapsed = time.time() - self.start_time
                speed = self.processed_count / elapsed if elapsed > 0 else 0
                logger.info(f"已处理 {self.processed_count} 个专利，速度: {speed:.2f} 个/秒")
            
            return True
            
        except Exception as e:
            logger.error(f"处理专利 {patent_no} 失败: {e}")
            self.failed_count += 1
            return False
    
    def process_chunk(self, patent_list: List[Tuple[str, int]], chunk_id: int):
        """处理一个数据块"""
        logger.info(f"开始处理数据块 {chunk_id}，共 {len(patent_list)} 个专利")
        
        for patent_no, year in patent_list:
            self.process_patent(patent_no, year)
            time.sleep(0.5)  # 减少延迟
        
        logger.info(f"数据块 {chunk_id} 处理完成")
    
    def save_results(self, year: int):
        """保存结果到 DTA 文件"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 保存自引专利
        if self.self_citing_data:
            # 去重
            seen = set()
            unique_self = []
            for item in self.self_citing_data:
                key = (item['source_patent'], item['citation_patent'])
                if key not in seen:
                    seen.add(key)
                    unique_self.append(item)
            
            self_citing_file = os.path.join(config.OUTPUT_DIR, f"self_citing_{year}_{timestamp}.dta")
            df = pd.DataFrame(unique_self)
            df.to_stata(self_citing_file, write_index=False)
            logger.info(f"自引专利数据已保存: {self_citing_file} (共 {len(unique_self)} 条)")
        
        # 保存他引专利
        if self.other_citing_data:
            # 去重
            seen = set()
            unique_other = []
            for item in self.other_citing_data:
                key = (item['source_patent'], item['citation_patent'])
                if key not in seen:
                    seen.add(key)
                    unique_other.append(item)
            
            other_citing_file = os.path.join(config.OUTPUT_DIR, f"other_citing_{year}_{timestamp}.dta")
            df = pd.DataFrame(unique_other)
            df.to_stata(other_citing_file, write_index=False)
            logger.info(f"他引专利数据已保存: {other_citing_file} (共 {len(unique_other)} 条)")
    
    def run(self, year: int, patent_list: List[Tuple[str, int]], chunk_size: int = 100):
        """
        运行爬虫
        
        Args:
            year: 年份
            patent_list: (专利号, 年份) 列表
            chunk_size: 每个数据块的大小
        """
        if not patent_list:
            logger.error("专利列表为空")
            return
        
        total = len(patent_list)
        logger.info(f"开始爬取 {year} 年的 {total} 个专利的引证数据")
        
        self.start_time = time.time()
        self.processed_count = 0
        self.failed_count = 0
        
        try:
            self.init_driver()
            
            if not self.login():
                logger.error("登录失败，程序退出")
                return
            
            # 分块处理
            chunks = [patent_list[i:i + chunk_size] for i in range(0, len(patent_list), chunk_size)]
            
            for i, chunk in enumerate(chunks):
                logger.info(f"处理第 {i+1}/{len(chunks)} 个数据块")
                self.process_chunk(chunk, i+1)
                
                # 每处理一个块保存一次中间结果
                self.save_results(year)
            
            # 保存最终结果
            self.save_results(year)
            
            elapsed = time.time() - self.start_time
            logger.info(f"爬取完成: 成功 {self.processed_count}/{total}，失败 {self.failed_count}，耗时 {elapsed:.2f} 秒")
            
        except Exception as e:
            logger.error(f"程序运行出错: {e}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            self.close()
    
    def close(self):
        """关闭浏览器"""
        if self.page:
            self.page.quit()
            logger.info("浏览器已关闭")


def load_patents_from_dta(filepath: str) -> List[Tuple[str, int]]:
    """
    从dta文件加载专利列表
    
    Returns:
        (专利号, 年份) 列表
    """
    try:
        # 从文件名提取年份
        filename = os.path.basename(filepath)
        match = re.search(r'(\d{4})年', filename)
        if match:
            year = int(match.group(1))
        else:
            logger.warning(f"无法从文件名提取年份: {filename}")
            year = 0
        
        # 读取dta文件
        df = pd.read_stata(filepath)
        
        # 提取专利号
        if 'pub_no' in df.columns:
            patents = df['pub_no'].dropna().unique().tolist()
        else:
            logger.error(f"文件 {filename} 中没有 'pub_no' 列")
            return []
        
        # 转换为 (专利号, 年份) 列表
        result = [(str(p).strip(), year) for p in patents if p]
        
        logger.info(f"从 {filename} 加载了 {len(result)} 个专利 (年份: {year})")
        return result
        
    except Exception as e:
        logger.error(f"加载文件失败 {filepath}: {e}")
        return []


def main():
    """主函数"""
    if config.USERNAME == "your_username_here" or config.PASSWORD == "your_password_here":
        print("请先修改 config.py 中的用户名和密码！")
        return
    
    # 查找所有dta文件
    input_dir = os.path.join(os.getcwd(), 'input')
    dta_files = sorted(glob.glob(os.path.join(input_dir, '上市公司专利_*.dta')))
    
    if not dta_files:
        print(f"未在 {input_dir} 目录找到dta文件")
        return
    
    print(f"找到 {len(dta_files)} 个数据文件")
    
    # 依次处理每个文件
    for dta_file in dta_files:
        patent_list = load_patents_from_dta(dta_file)
        
        if not patent_list:
            continue
        
        # 获取年份
        year = patent_list[0][1]
        
        print(f"\n{'='*60}")
        print(f"开始处理 {year} 年的数据，共 {len(patent_list)} 个专利")
        print(f"{'='*60}\n")
        
        crawler = IncoPatCrawler()
        crawler.run(year, patent_list, chunk_size=100)
        
        print(f"\n{year} 年数据处理完成\n")


if __name__ == "__main__":
    main()
