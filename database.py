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

    def join_query(self, table_names, join_conditions=None, select_fields="*", where_conditions=None, limit=None):
        """
        执行联表查询
        
        Args:
            table_names (list): 要连接的表名列表
            join_conditions (list): 连接条件列表，格式为 [("table1.field", "table2.field"), ...]
            select_fields (str): 要选择的字段，默认为"*"
            where_conditions (str): WHERE条件语句
            limit (int): 限制返回记录数
            
        Returns:
            dict: 包含查询结果和总记录数的字典
        """
        if not table_names or len(table_names) < 2:
            print("❌ 至少需要两个表进行联表查询")
            return {"data": [], "total": 0}

        try:
            # 构建JOIN语句
            join_parts = []
            for i in range(1, len(table_names)):
                if join_conditions and i-1 < len(join_conditions):
                    condition = join_conditions[i-1]
                    if isinstance(condition, tuple) and len(condition) == 2:
                        join_parts.append(f"JOIN {table_names[i]} ON {condition[0]} = {condition[1]}")
                    else:
                        # 默认使用id字段连接
                        join_parts.append(f"JOIN {table_names[i]} ON {table_names[0]}.id = {table_names[i]}.id")
                else:
                    # 默认使用id字段连接
                    join_parts.append(f"JOIN {table_names[i]} ON {table_names[0]}.id = {table_names[i]}.id")
            
            # 构建完整SQL
            sql = f"SELECT {select_fields} FROM {table_names[0]} " + " ".join(join_parts)
            
            # 添加WHERE条件
            if where_conditions:
                sql += f" WHERE {where_conditions}"
                
            # 添加LIMIT
            if limit:
                sql += f" LIMIT {limit}"
            
            print(f"🔍 执行联表查询: {sql}")
            
            with self.engine.connect() as conn:
                # 执行查询
                result = conn.execute(text(sql))
                data = []
                for row in result:
                    # 正确处理SQLAlchemy行对象
                    row_dict = dict(row._mapping)
                    data.append(row_dict)
                
                # 获取记录总数
                count_sql = f"SELECT COUNT(*) FROM {table_names[0]} " + " ".join(join_parts)
                if where_conditions:
                    count_sql += f" WHERE {where_conditions}"
                    
                count_result = conn.execute(text(count_sql))
                total_count = count_result.scalar()
                
                return {"data": data, "total": total_count}
        except Exception as e:
            print(f"❌ 联表查询失败: {str(e)}")
            import traceback
            traceback.print_exc()
            return {"data": [], "total": 0}

    def complex_query(self, sql_query, params=None):
        """
        执行复杂自定义查询
        
        Args:
            sql_query (str): SQL查询语句
            params (dict): 查询参数
            
        Returns:
            dict: 包含查询结果和总记录数的字典
        """
        try:
            with self.engine.connect() as conn:
                # 执行查询
                if params:
                    result = conn.execute(text(sql_query), params)
                else:
                    result = conn.execute(text(sql_query))
                    
                data = []
                for row in result:
                    # 正确处理SQLAlchemy行对象
                    row_dict = dict(row._mapping)
                    data.append(row_dict)
                
                return {"data": data, "total": len(data)}
        except Exception as e:
            print(f"❌ 复杂查询失败: {str(e)}")
            import traceback
            traceback.print_exc()
            return {"data": [], "total": 0}

if __name__ == "__main__":
    db = DatabaseManager()
    db.test_connection()