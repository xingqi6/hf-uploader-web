FROM python:3.10-slim

WORKDIR /app

# 设置环境变量防止中文乱码和缓存
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONUTF8=1
ENV LANG=C.UTF-8

# 安装依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# 复制所有代码
COPY . .

# 创建挂载点
RUN mkdir -p /app/config /app/data

EXPOSE 7860

# 核心：关闭Debug和Reloader防止无限重启
CMD ["python", "app.py"]
