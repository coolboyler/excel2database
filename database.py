from sqlalchemy import create_engine
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
    
    def test_connection(self):
        """测试数据库连接"""
        try:
            with self.engine.connect() as conn:
                print("✅ 数据库连接成功")
                return True
        except Exception as e:
            print(f"❌ 数据库连接失败: {e}")
            return False
    
if __name__ == "__main__":
    db = DatabaseManager()
    db.test_connection()