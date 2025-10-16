# database.py
from sqlalchemy import create_engine, text
from config import DB_CONFIG

class DatabaseManager:
    def __init__(self):
        self.engine = self.create_engine()
    
    def create_engine(self):
        """创建数据库引擎"""
        connection_string = (
            f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
            f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        return create_engine(connection_string)
    
    def get_engine(self):
        """获取数据库引擎"""
        return self.engine
        
    def test_connection(self):
        """测试数据库连接"""
        try:
            with self.engine.connect() as conn:
                print("✅ 数据库连接成功")
                return True
        except Exception as e:
            print(f"❌ 数据库连接失败: {e}")
            return False
            
    def create_power_table(self, engine, table_name):
        """创建电力数据表（如果不存在）"""
        with engine.connect() as conn:
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    data_date DATE NOT NULL,
                    data_hour INT NOT NULL,
                    data_type VARCHAR(50) NOT NULL,
                    area_name VARCHAR(50) NOT NULL,
                    power_value FLOAT NOT NULL,
                    created_at DATETIME NOT NULL,
                    INDEX idx_date_hour (data_date, data_hour)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """))
            
    def save_power_data(self, records, data_date):
        """保存电力数据到数据库"""
        if not records:
            return False, None, 0, []
            
        # 获取第一条记录的类型
        data_type = records[0].get('data_type', '')
        
        # 根据类型确定表名
        if '负荷实际信息' in data_type:
            table_name = 'power_actual'
        elif '负荷预测信息' in data_type:
            table_name = 'power_forecast'
        else:
            table_name = 'power_data'
            
        try:
            # 连接数据库
            engine = self.get_engine()
            if not engine:
                return False, None, 0, []
                
            # 创建表（如果不存在）
            self.create_power_table(engine, table_name)
            
            # 插入数据
            with engine.connect() as conn:
                # 开始事务
                with conn.begin():
                    # 删除同一天的数据
                    conn.execute(
                        text(f"DELETE FROM {table_name} WHERE data_date = :data_date"),
                        {"data_date": data_date}
                    )
                    
                    # 批量插入
                    for record in records:
                        conn.execute(
                            text(f"""
                                INSERT INTO {table_name} 
                                (data_date, data_hour, data_type, area_name, power_value, created_at)
                                VALUES 
                                (:data_date, :data_hour, :data_type, :area_name, :power_value, NOW())
                            """),
                            record
                        )
                
                # 获取前5行数据
                result = conn.execute(
                    text(f"SELECT * FROM {table_name} WHERE data_date = :data_date LIMIT 5"),
                    {"data_date": data_date}
                )
                preview_data = [dict(row._mapping) for row in result]
            
            print(f"✅ 成功导入 {len(records)} 条记录到 {table_name} 表")
            return True, table_name, len(records), preview_data
            
        except Exception as e:
            print(f"❌ 数据库错误: {str(e)}")
            return False, None, 0, []
    
    def get_tables(self):
        """获取所有数据表"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SHOW TABLES"))
                tables = [row[0] for row in result]
                return tables
        except Exception as e:
            print(f"❌ 获取数据表失败: {str(e)}")
            return []
            
    def get_table_data(self, table_name, limit=5):
        """获取指定表的数据"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT * FROM {table_name} LIMIT {limit}"))
                data = []
                for row in result:
                    # 修复：正确处理SQLAlchemy行对象
                    row_dict = dict(row._mapping)
                    # 特别处理 record_time 字段
                    if "record_time" in row_dict and row_dict["record_time"]:
                        # 如果是 timedelta 对象
                        if hasattr(row_dict["record_time"], 'seconds'):
                            hours = row_dict["record_time"].seconds // 3600
                            minutes = (row_dict["record_time"].seconds % 3600) // 60
                            row_dict["record_time"] = f"{hours:02d}:{minutes:02d}"
                        # 如果是 datetime.time 对象
                        elif hasattr(row_dict["record_time"], 'strftime'):
                            row_dict["record_time"] = row_dict["record_time"].strftime("%H:%M")
                    data.append(row_dict)
                
                # 获取记录总数
                count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                total_count = count_result.scalar()
                
                return {"data": data, "total": total_count}
        except Exception as e:
            print(f"❌ 获取表数据失败: {str(e)}")
            return {"data": [], "total": 0}
                
    def delete_table(self, table_name):
        """删除指定表"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                return True
        except Exception as e:
            print(f"❌ 删除表失败: {str(e)}")
            return False

if __name__ == "__main__":
    db = DatabaseManager()
    db.test_connection()