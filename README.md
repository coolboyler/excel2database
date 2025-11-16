# Excel to SQL 导入工具

这是一个可以将各种格式的Excel文件导入到MySQL数据库的工具，支持Web界面操作和脚本自动导入两种方式。

## 功能特点

- 支持多种Excel格式文件导入
- 自动生成数据库表结构
- 提供Web界面可视化操作
- 支持脚本自动导入（新增）
- 支持批量导入多个文件（新增）
- 提供浏览器自动化测试接口（新增）

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

## 安装部署

1. 安装依赖：
```bash
pip install -r requirements.txt
```

2. 配置数据库连接：
修改 `config.py` 文件中的数据库连接配置

3. 启动服务：
```bash
python main.py
```
或者
```bash
uvicorn api:app --reload
```

## Web界面使用

启动服务后，访问 `http://localhost:8000` 即可使用Web界面进行文件上传和导入操作。

## 脚本自动导入使用方法（新增功能）

除了Web界面，还提供了命令行脚本支持自动导入，无需人工干预。

### 使用方法

1. 确保API服务正在运行：
```bash
uvicorn api:app --host 0.0.0.0 --port 8000
```

2. 使用script_import.py脚本进行导入：

查看可用的文件列表：
```bash
python script_import.py --list
```

导入特定文件：
```bash
python script_import.py --file "负荷实际信息(2023-01-01).xlsx"
```

导入所有可用文件：
```bash
python script_import.py --all
```

指定API服务器地址（默认为http://localhost:8000）：
```bash
python script_import.py --all --url http://your-server-address:port
```

### 脚本参数说明

- `--url`: API服务器地址，默认为 `http://localhost:8000`
- `--file` 或 `-f`: 指定要导入的特定文件名
- `--all` 或 `-a`: 导入所有可用文件
- `--list` 或 `-l`: 列出所有可用文件

## 浏览器自动化接口（新增功能）

为支持浏览器自动化测试，提供了专门的API接口：

### 单文件导入接口

```
POST /script_import
参数: filename (表单数据)
说明: 导入指定的Excel文件
```

示例：
```python
import requests

response = requests.post('http://localhost:8000/script_import', data={'filename': '负荷实际信息(2023-01-01).xlsx'})
result = response.json()
```

### 全自动导入接口

```
POST /auto_import
说明: 自动导入data目录下的所有Excel文件
```

示例：
```python
import requests

response = requests.post('http://localhost:8000/auto_import')
result = response.json()
```

## 支持的Excel文件格式

1. 负荷实际信息
2. 负荷预测信息
3. 信息披露(区域)查询实际信息
4. 信息披露(区域)查询预测信息
5. 实时节点电价查询
6. 日前节点电价查询

## 注意事项

- Excel文件需要放置在 `data` 目录下
- 确保数据库连接配置正确
- 脚本自动导入需要API服务正常运行

## 贡献

欢迎提交Issue和Pull Request来改进这个项目。

## 许可证

本项目仅供学习和参考使用。