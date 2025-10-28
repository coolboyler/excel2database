# Excel2SQL - Excel数据导入工具

Excel2SQL 是一个将Excel数据导入MySQL数据库的工具，支持多种Excel格式的自动识别和处理，提供了Web界面和命令行两种操作方式。

## 功能特性

- **多种Excel格式支持**：自动识别并处理不同格式的Excel文件
- **Web管理界面**：提供友好的Web界面进行文件上传、数据导入和查询操作
- **批量上传**：支持拖拽和选择文件两种方式批量上传Excel文件
- **重复文件检测**：上传时自动检测重复文件并提示用户确认是否覆盖
- **多条件数据查询**：支持按字段进行多条件查询和数据筛选
- **数据导出**：支持将查询结果导出为CSV格式
- **联表查询**：支持对多个数据表进行联合查询
- **实时节点电价处理**：专门处理实时和日前节点电价数据
- **自动生成表结构**：根据数据内容自动创建相应的数据库表

## 技术栈

- **后端**：Python + FastAPI
- **数据库**：MySQL
- **前端**：HTML + CSS + JavaScript
- **数据处理**：Pandas
- **数据库ORM**：SQLAlchemy

## 环境要求

- Python 3.7+
- MySQL 5.7+
- pip (Python包管理工具)

## 安装步骤

1. 克隆项目代码：
```bash
git clone <repository-url>
cd excel2sql
```

2. 安装依赖：
```bash
pip install -r requirements.txt
```

3. 配置数据库：
   修改 `config.py` 文件中的数据库连接信息：
   ```python
   DB_CONFIG = {
       'host': 'localhost',        # 数据库主机地址
       'port': 33069,              # 数据库端口
       'user': 'root',             # 用户名
       'password': 'root',         # 密码
       'database': 'power_management',  # 数据库名
       'charset': 'utf8mb4'        # 字符集
   }
   ```

4. 启动服务：
```bash
python main.py    # 命令行方式处理data目录下的文件
# 或
python api.py     # 启动Web服务（默认端口8000）
```

## 使用说明

### 命令行方式

将需要处理的Excel文件放入 `data` 目录，然后运行：
```bash
python main.py
```
程序会自动处理目录下的所有Excel文件，并将数据导入数据库。

### Web界面方式

1. 启动Web服务：
```bash
python api.py
```

2. 在浏览器中访问 `http://localhost:8000`

3. 使用Web界面功能：
   - **上传文件**：通过点击选择文件或拖拽方式上传Excel文件
   - **导入数据**：对已上传的文件进行数据导入
   - **数据查询**：对导入的数据进行多条件查询
   - **导出数据**：将查询结果导出为CSV文件
   - **联表查询**：对多个表进行联合查询分析

## 支持的Excel格式

1. **负荷实际信息/负荷预测信息**：包含负荷数据的Excel文件
2. **信息披露(区域)查询实际信息/预测信息**：包含区域电力信息披露的Excel文件
3. **实时节点电价查询/日前节点电价查询**：包含节点电价数据的Excel文件
4. **其他格式**：工具会尝试自动识别并处理其他格式的Excel文件

## 项目结构

```
excel2sql/
├── api.py              # Web API接口
├── main.py             # 命令行主程序
├── config.py           # 配置文件
├── database.py         # 数据库管理模块
├── pred_reader.py      # Excel数据读取和处理模块
├── requirements.txt    # 项目依赖
├── data/               # Excel文件存放目录
├── static/             # Web静态资源
│   ├── css/
│   └── js/
├── templates/          # Web页面模板
│   ├── index.html      # 主页面
│   ├── table_query.html# 表查询页面
│   └── join_query.html # 联表查询页面
└── created/            # 导出文件存放目录
```

## 数据库表结构

工具会根据导入的数据自动创建相应的数据表，主要包括：

1. **电力数据表**：存储负荷、电价等电力相关数据
2. **设备信息表**：存储电力设备相关信息
3. **机组约束表**：存储机组约束配置信息
4. **设备电压等级表**：存储设备电压等级信息

## 注意事项

1. 确保MySQL数据库服务正常运行
2. 根据实际环境修改数据库配置信息
3. Excel文件需要符合特定格式才能被正确识别和处理
4. 大文件导入可能需要较长时间，请耐心等待
5. 重复导入同名文件时会提示确认是否覆盖

## 常见问题

**Q: 导入Excel文件时出现编码错误怎么办？**
A: 确保Excel文件保存为标准格式，推荐使用.xlsx格式。

**Q: 数据库连接失败怎么办？**
A: 检查config.py中的数据库配置信息是否正确，确保MySQL服务正在运行。

**Q: Web界面无法访问怎么办？**
A: 确认api.py已正确启动，默认访问地址为 http://localhost:8000

## 贡献

欢迎提交Issue和Pull Request来改进这个项目。

## 许可证

本项目仅供学习和参考使用。