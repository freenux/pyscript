import asyncio
import csv
import json
import aiohttp
import aiomysql
from datetime import datetime
from typing import List, Dict, Optional
import logging
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 配置信息
DB_CONFIG = {
    'host': 'localhost',
    'user': 'your_username',
    'password': 'your_password',
    'db': 'your_database',
    'charset': 'utf8mb4'
}

APPLE_API_CONFIG = {
    'key_id': 'your_key_id',
    'issuer_id': 'your_issuer_id',
    'bundle_id': 'your_bundle_id',
    'private_key_path': 'path_to_your_private_key.p8'
}

class AppleOrderQuery:
    def __init__(self, input_csv: str, output_file: str):
        self.input_csv = input_csv
        self.output_file = output_file
        self.batch_size = 1000  # 每批处理的订单数量
        self.max_concurrent_requests = 10  # 最大并发请求数
        self.semaphore = asyncio.Semaphore(self.max_concurrent_requests)

    async def get_db_connection(self, table_suffix: str) -> aiomysql.Connection:
        return await aiomysql.connect(**DB_CONFIG)

    async def query_payment_orders(self, order_ids: List[str], qids: List[int]) -> Dict[str, str]:
        """查询支付系统订单"""
        results = {}
        # 按用户ID后两位分组
        qid_groups = {}
        for order_id, qid in zip(order_ids, qids):
            suffix = str(qid % 100).zfill(2)
            if suffix not in qid_groups:
                qid_groups[suffix] = []
            qid_groups[suffix].append((order_id, qid))

        # 并发查询每个分表
        tasks = []
        for suffix, group in qid_groups.items():
            conn = await self.get_db_connection(suffix)
            try:
                async with conn.cursor() as cur:
                    # 构建IN查询
                    order_id_list = [item[0] for item in group]
                    qid_list = [item[1] for item in group]
                    query = f"""
                        SELECT trans_id, up_order_num 
                        FROM t_translog_{suffix}
                        WHERE trans_id IN ({','.join(['%s'] * len(order_id_list))})
                        AND qid IN ({','.join(['%s'] * len(qid_list))})
                    """
                    await cur.execute(query, order_id_list + qid_list)
                    rows = await cur.fetchall()
                    for row in rows:
                        results[str(row[0])] = row[1]
            finally:
                conn.close()

        return results

    async def query_apple_order(self, transaction_id: str) -> Optional[Dict]:
        """查询苹果订单详情"""
        async with self.semaphore:
            try:
                # 这里需要实现具体的Apple Connect API调用
                # 示例代码，需要替换为实际的API实现
                async with aiohttp.ClientSession() as session:
                    headers = {
                        'Authorization': f'Bearer {self._get_apple_token()}',
                        'Content-Type': 'application/json'
                    }
                    url = f'https://api.storekit.itunes.apple.com/inApps/v2/transactions/{transaction_id}'
                    async with session.get(url, headers=headers) as response:
                        if response.status == 200:
                            return await response.json()
                        else:
                            logger.error(f"Failed to query Apple order {transaction_id}: {response.status}")
                            return None
            except Exception as e:
                logger.error(f"Error querying Apple order {transaction_id}: {str(e)}")
                return None

    def _get_apple_token(self) -> str:
        """获取Apple Connect API的认证token"""
        # 这里需要实现具体的token生成逻辑
        pass

    async def process_batch(self, batch: List[Dict]) -> None:
        """处理一批订单"""
        order_ids = [item['id'] for item in batch]
        qids = [item['qid'] for item in batch]
        
        # 查询支付系统订单
        payment_results = await self.query_payment_orders(order_ids, qids)
        
        # 查询苹果订单
        tasks = []
        for order_id, apple_transaction_id in payment_results.items():
            if apple_transaction_id:
                tasks.append(self.query_apple_order(apple_transaction_id))
        
        apple_results = await asyncio.gather(*tasks)
        
        # 写入结果
        with open(self.output_file, 'a') as f:
            for order_id, apple_result in zip(payment_results.keys(), apple_results):
                if apple_result:
                    result = {
                        'order_id': order_id,
                        'apple_transaction_id': payment_results[order_id],
                        'apple_order_details': apple_result
                    }
                    f.write(json.dumps(result) + '\n')

    async def run(self):
        """主运行函数"""
        try:
            with open(self.input_csv, 'r') as f:
                reader = csv.DictReader(f)
                batch = []
                
                for row in reader:
                    batch.append(row)
                    if len(batch) >= self.batch_size:
                        await self.process_batch(batch)
                        batch = []
                
                # 处理剩余的订单
                if batch:
                    await self.process_batch(batch)
                    
        except Exception as e:
            logger.error(f"Error processing orders: {str(e)}")
            raise

def main():
    input_csv = 'orders.csv'
    output_file = 'apple_order_results.jsonl'
    
    query = AppleOrderQuery(input_csv, output_file)
    asyncio.run(query.run())

if __name__ == '__main__':
    main() 