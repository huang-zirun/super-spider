"""
处理incoPat导出的Excel文件，提取引证专利数据并保存为DTA格式

使用方法：
1. 在incoPat网站上手动导出每个专利的自引和他引Excel文件
2. 将文件保存到 downloads 目录，命名格式：
   - 自引：self_citing_{专利号}.xlsx
   - 他引：other_citing_{专利号}.xlsx
3. 运行此脚本：python process_excel.py
"""

import os
import glob
import pandas as pd
from typing import List, Dict
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_excel_file(excel_path: str) -> List[Dict]:
    """
    解析Excel文件，提取专利号和IPC分类号
    
    Args:
        excel_path: Excel文件路径
        
    Returns:
        专利信息列表
    """
    patent_list = []
    
    try:
        # 读取Excel文件
        df = pd.read_excel(excel_path)
        
        if df.empty:
            logger.warning(f"Excel文件为空: {excel_path}")
            return []
        
        logger.info(f"文件 {os.path.basename(excel_path)} 共有 {len(df)} 行，列名: {list(df.columns)}")
        
        # 查找专利号列和IPC列
        patent_no_col = None
        ipc_col = None
        
        for col in df.columns:
            col_str = str(col).upper()
            # 专利号列的可能名称
            if any(keyword in col_str for keyword in ['公开', '公告号', '专利号', 'PN', 'PUBLICATION', 'PUB_NO']):
                patent_no_col = col
                logger.info(f"找到专利号列: {col}")
            # IPC列的可能名称
            if any(keyword in col_str for keyword in ['IPC', '分类号', 'CLASSIFICATION', '主分类']):
                ipc_col = col
                logger.info(f"找到IPC列: {col}")
        
        # 如果没找到，尝试根据列位置推断
        if patent_no_col is None and len(df.columns) >= 3:
            patent_no_col = df.columns[2]  # 第3列通常是公开号
            logger.info(f"使用第3列作为专利号列: {patent_no_col}")
        
        if ipc_col is None and len(df.columns) >= 9:
            ipc_col = df.columns[8]  # IPC分类号通常在后面的列
            logger.info(f"使用第9列作为IPC列: {ipc_col}")
        
        # 提取数据
        for _, row in df.iterrows():
            try:
                patent_no = str(row[patent_no_col]) if patent_no_col else ""
                ipc = str(row[ipc_col]) if ipc_col else ""
                
                # 清理数据
                patent_no = patent_no.strip() if patent_no and patent_no != 'nan' else ""
                ipc = ipc.strip() if ipc and ipc != 'nan' else ""
                
                # 验证专利号格式
                if patent_no and len(patent_no) >= 5:
                    # 检查是否以国家代码开头
                    if any(patent_no.startswith(code) for code in ['CN', 'US', 'EP', 'WO', 'JP', 'KR']):
                        patent_list.append({
                            'patent_no': patent_no,
                            'ipc': ipc
                        })
            except Exception as e:
                continue
        
        # 去重
        seen = set()
        unique_list = []
        for item in patent_list:
            if item['patent_no'] not in seen:
                seen.add(item['patent_no'])
                unique_list.append(item)
        
        logger.info(f"解析完成: 共 {len(unique_list)} 条唯一专利")
        return unique_list
        
    except Exception as e:
        logger.error(f"解析Excel失败 {excel_path}: {e}")
        return []


def process_all_excel_files():
    """处理所有Excel文件并保存为DTA格式"""
    
    download_dir = os.path.join(os.getcwd(), 'downloads')
    output_dir = os.path.join(os.getcwd(), 'output')
    
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 查找所有自引和他引Excel文件
    self_citing_files = glob.glob(os.path.join(download_dir, 'self_citing_*.xls*'))
    other_citing_files = glob.glob(os.path.join(download_dir, 'other_citing_*.xls*'))
    
    logger.info(f"找到 {len(self_citing_files)} 个自引文件")
    logger.info(f"找到 {len(other_citing_files)} 个他引文件")
    
    # 处理自引文件
    self_citing_data = []
    for file_path in self_citing_files:
        # 从文件名提取源专利号
        filename = os.path.basename(file_path)
        # 格式: self_citing_{专利号}.xlsx
        source_patent = filename.replace('self_citing_', '').replace('.xlsx', '').replace('.xls', '')
        
        # 解析Excel
        patent_list = parse_excel_file(file_path)
        
        # 添加到数据列表
        for info in patent_list:
            self_citing_data.append({
                'source_patent': source_patent,
                'citation_patent': info['patent_no'],
                'ipc': info['ipc']
            })
    
    # 处理他引文件
    other_citing_data = []
    for file_path in other_citing_files:
        # 从文件名提取源专利号
        filename = os.path.basename(file_path)
        # 格式: other_citing_{专利号}.xlsx
        source_patent = filename.replace('other_citing_', '').replace('.xlsx', '').replace('.xls', '')
        
        # 解析Excel
        patent_list = parse_excel_file(file_path)
        
        # 添加到数据列表
        for info in patent_list:
            other_citing_data.append({
                'source_patent': source_patent,
                'citation_patent': info['patent_no'],
                'ipc': info['ipc']
            })
    
    # 去重（基于源专利和引证专利的组合）
    seen_self = set()
    unique_self = []
    for item in self_citing_data:
        key = (item['source_patent'], item['citation_patent'])
        if key not in seen_self:
            seen_self.add(key)
            unique_self.append(item)
    
    seen_other = set()
    unique_other = []
    for item in other_citing_data:
        key = (item['source_patent'], item['citation_patent'])
        if key not in seen_other:
            seen_other.add(key)
            unique_other.append(item)
    
    # 保存为DTA文件
    if unique_self:
        self_citing_file = os.path.join(output_dir, 'self_citing_data.dta')
        df_self = pd.DataFrame(unique_self)
        df_self.to_stata(self_citing_file, write_index=False)
        logger.info(f"自引专利数据已保存: {self_citing_file}")
        logger.info(f"  - 总记录数: {len(unique_self)} (去重前: {len(self_citing_data)})")
        logger.info(f"  - 源专利数: {len(set(item['source_patent'] for item in unique_self))}")
    else:
        logger.warning("没有自引数据")
    
    if unique_other:
        other_citing_file = os.path.join(output_dir, 'other_citing_data.dta')
        df_other = pd.DataFrame(unique_other)
        df_other.to_stata(other_citing_file, write_index=False)
        logger.info(f"他引专利数据已保存: {other_citing_file}")
        logger.info(f"  - 总记录数: {len(unique_other)} (去重前: {len(other_citing_data)})")
        logger.info(f"  - 源专利数: {len(set(item['source_patent'] for item in unique_other))}")
    else:
        logger.warning("没有他引数据")
    
    logger.info("处理完成！")


if __name__ == "__main__":
    process_all_excel_files()
