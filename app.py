# app.py (V35.0 深度扫描 & 教程复原版)
import os
import sys
import time
import json
import threading
import smtplib
import logging
import queue
import shutil
from email.mime.text import MIMEText
from email.header import Header
from email.utils import formataddr
from flask import Flask, render_template, request, jsonify, Response
from huggingface_hub import HfApi

# 强制 UTF-8
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

app = Flask(__name__)

# ================= 全局配置 =================
CONFIG_FILE = "/app/config/settings.json"
DATA_DIR = "/app/data"
LOG_QUEUE = queue.Queue(maxsize=500)
FAILURE_RECORD_FILE = "/app/config/failures.json"

DEFAULT_CONFIG = {
    "hf_endpoint": "https://hf-mirror.com", 
    "hf_token": "", "repo_id": "", "repo_type": "dataset", "remote_folder": "",
    "email_host": "", "email_port": "", "email_user": "", "email_pass": "", "email_to": "",
    "warn_timeout": 900, "kill_timeout": 1800, "idle_interval": 1800,
    "max_retries": 5, "notify_min_size": 1024, "file_interval": 15, 
    "delete_after_upload": True,
    "enable_hf_transfer": False,
    "enable_idle_email": False
}

uploader_thread = None
stop_event = threading.Event()
is_running = False

# 日志配置
class QueueHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            if LOG_QUEUE.full():
                try: LOG_QUEUE.get_nowait()
                except: pass
            LOG_QUEUE.put(msg)
        except: pass

logger = logging.getLogger("HF_Uploader")
logger.setLevel(logging.INFO)
web_formatter = logging.Formatter('%(message)s') 
q_handler = QueueHandler()
q_handler.setFormatter(web_formatter)
logger.addHandler(q_handler)

console_formatter = logging.Formatter('%(asctime)s - %(message)s', datefmt='%H:%M:%S')
console_handler = logging.StreamHandler()
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

JUNK_FILES = {'.DS_Store', 'Thumbs.db', 'desktop.ini', '@eaDir', '.smbdelete'}

def load_config():
    if not os.path.exists(CONFIG_FILE): return DEFAULT_CONFIG.copy()
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            config = DEFAULT_CONFIG.copy()
            if "enable_hf_transfer" not in config: config["enable_hf_transfer"] = False
            if "enable_idle_email" not in config: config["enable_idle_email"] = False
            config.update(data)
            return config
    except: return DEFAULT_CONFIG.copy()

def save_config(config):
    try:
        os.makedirs(os.path.dirname(CONFIG_FILE), exist_ok=True)
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
        return True
    except: return False

def load_failures():
    if not os.path.exists(FAILURE_RECORD_FILE): return {}
    try:
        with open(FAILURE_RECORD_FILE, 'r', encoding='utf-8') as f: return json.load(f)
    except: return {}

def save_failures(data):
    try:
        with open(FAILURE_RECORD_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except: pass

def safe_int(value, default):
    try:
        if value is None or str(value).strip() == "": return default
        return int(value)
    except: return default

def send_email(cfg, title, content):
    if not cfg.get('email_user') or not cfg.get('email_pass'): return
    try:
        formatted = content.replace('\n', '<br>')
        time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        msg = MIMEText(f"<h3>{title}</h3><p>{formatted}</p><hr><p style='font-size:12px;color:gray'>{time_str} | NAS助手</p>", 'html', 'utf-8')
        msg['From'] = formataddr(("NAS助手", cfg['email_user']))
        msg['To'] = formataddr(("我", cfg['email_to']))
        msg['Subject'] = Header(title, 'utf-8')
        
        host = cfg.get('email_host') if cfg.get('email_host') else "smtp.qq.com"
        port = safe_int(cfg.get('email_port'), 465)
        
        smtp = smtplib.SMTP_SSL(host, port, timeout=30)
        smtp.login(cfg['email_user'], cfg['email_pass'])
        smtp.sendmail(cfg['email_user'], [cfg['email_to']], msg.as_string())
        smtp.quit()
        logger.info(f"📧 [邮件] 发送成功: {title}")
    except Exception as e:
        logger.error(f"⚠️ [邮件] 发送失败: {str(e)}")

def recursive_delete_empty(path):
    try:
        if path == DATA_DIR or not path.startswith(DATA_DIR): return
        if os.path.isdir(path):
            files = os.listdir(path)
            valid = [f for f in files if f not in JUNK_FILES and not f.startswith('.')]
            if not valid:
                for f in files:
                    try:
                        p = os.path.join(path, f)
                        if os.path.isdir(p): shutil.rmtree(p)
                        else: os.remove(p)
                    except: pass
                os.rmdir(path)
                logger.info(f"🧹 [清理] 空文件夹已删除: {os.path.basename(path)}")
                recursive_delete_empty(os.path.dirname(path))
    except: pass

def check_remote_success(api, repo_id, repo_type, remote_path, local_size):
    try:
        info = api.get_paths_info(
            repo_id=repo_id,
            repo_type=repo_type,
            paths=[remote_path],
        )
        if len(info) > 0:
            remote_size = info[0].size
            if remote_size == local_size:
                return True
    except:
        return False
    return False

def uploader_daemon(config):
    global is_running
    endpoint = config.get('hf_endpoint', 'https://hf-mirror.com')
    
    # 🌟 速度模式控制
    use_accel = config.get('enable_hf_transfer', False)
    mode_str = "🚀 高速模式 (hf_transfer)" if use_accel else "🐢 稳定模式 (HTTP)"
    
    logger.info(f"🚀 服务启动 | 目标: {endpoint} | {mode_str}")
    
    os.environ["HF_ENDPOINT"] = endpoint
    if use_accel:
        os.environ["HF_HUB_ENABLE_HF_TRANSFER"] = "1"
    else:
        if "HF_HUB_ENABLE_HF_TRANSFER" in os.environ:
            del os.environ["HF_HUB_ENABLE_HF_TRANSFER"]
    
    try:
        api = HfApi(token=config['hf_token'], endpoint=endpoint)
        user = api.whoami()
        logger.info(f"✅ 登录成功: {user['name']}")
    except Exception as e:
        logger.error(f"❌ 登录失败: {str(e)}")
        is_running = False
        return

    history_file = os.path.join(os.path.dirname(CONFIG_FILE), "history.json")
    uploaded_files = set()
    if os.path.exists(history_file):
        try:
            with open(history_file, 'r') as f: uploaded_files = set(json.load(f))
        except: pass

    last_busy = time.time()
    last_idle = 0
    is_idle_mode = False

    while not stop_event.is_set():
        try:
            # 🌟 增强扫描日志：打印正在扫描哪里
            logger.debug(f"🔍 正在扫描: {DATA_DIR}...") 
            
            all_files = []
            for root, dirs, files in os.walk(DATA_DIR):
                for file in files:
                    if file.startswith('.') or file.endswith('.json'): continue
                    if file in JUNK_FILES: continue
                    full = os.path.join(root, file)
                    rel = os.path.relpath(full, DATA_DIR).replace("\\", "/")
                    if rel not in uploaded_files:
                        all_files.append((full, rel))

            if all_files:
                is_idle_mode = False
                tasks_by_folder = {}
                for full, rel in all_files:
                    folder = os.path.dirname(rel)
                    if not folder: folder = "根目录"
                    if folder not in tasks_by_folder: tasks_by_folder[folder] = []
                    tasks_by_folder[folder].append((full, rel))

                logger.info(f"📦 扫描完成: 发现 {len(all_files)} 个新文件")
                failures_db = load_failures()

                for folder_name, tasks in tasks_by_folder.items():
                    if stop_event.is_set(): break
                    logger.info(f"📂 [目录] 处理中: {folder_name}")
                    folder_success_count = 0
                    tasks.sort(key=lambda x: x[1])

                    for i, (local_p, rel_p) in enumerate(tasks):
                        if stop_event.is_set(): break
                        
                        file_name = os.path.basename(rel_p)
                        # 检查文件是否还在写入
                        s1 = os.path.getsize(local_p)
                        time.sleep(2)
                        if os.path.getsize(local_p) != s1:
                            logger.info(f"⏳ [跳过] 文件正在写入: {file_name}")
                            continue

                        # 避障等待
                        if i > 0: time.sleep(safe_int(config.get('file_interval'), 15))

                        remote_f = config.get('remote_folder', '')
                        if not remote_f or remote_f.strip() == "": remote_f = "."
                        remote_p = f"{remote_f}/{rel_p}" if remote_f != "." else rel_p
                        size_mb = s1 / (1024*1024)

                        logger.info(f"▶ [开始] 上传: {file_name} ({size_mb:.1f} MB)")

                        success = False
                        max_retries = safe_int(config.get('max_retries'), 5)
                        
                        for attempt in range(max_retries):
                            if stop_event.is_set(): break
                            try:
                                api.upload_file(
                                    path_or_fileobj=local_p, 
                                    path_in_repo=remote_p,
                                    repo_id=config['repo_id'],
                                    repo_type=config['repo_type'],
                                    token=config['hf_token']
                                )
                                success = True
                                break
                            except Exception as e:
                                err_str = str(e)
                                
                                # 智能捡漏：报错后去查房
                                logger.info(f"⚠️ 报错，核实远程文件...")
                                if check_remote_success(api, config['repo_id'], config['repo_type'], remote_p, s1):
                                    logger.info(f"🎉 [捡漏] 远程文件已存在且完整，视为成功！")
                                    success = True
                                    break
                                
                                backoff = 30 * (2 ** attempt)
                                logger.warning(f"❌ [重试] 第{attempt+1}次失败，休息 {backoff}秒... 原因: {err_str[:50]}")
                                time.sleep(backoff)
                                
                                if "401" in err_str or "403" in err_str:
                                    try: api = HfApi(token=config['hf_token'], endpoint=endpoint)
                                    except: pass

                        if success:
                            logger.info(f"✅ [成功] 任务完成: {file_name}")
                            uploaded_files.add(rel_p)
                            with open(history_file, 'w') as f: json.dump(list(uploaded_files), f)
                            
                            if rel_p in failures_db:
                                del failures_db[rel_p]
                                save_failures(failures_db)
                                logger.info(f"🧹 [记录] 已清除历史报错")

                            folder_success_count += 1
                            
                            if size_mb >= safe_int(config.get('notify_min_size'), 1024):
                                send_email(config, "大文件上传成功", f"文件: {rel_p}")

                            if config.get('delete_after_upload', True):
                                try:
                                    os.remove(local_p)
                                    logger.info(f"🗑️ [删除] 本地文件: {file_name}")
                                    recursive_delete_empty(os.path.dirname(local_p))
                                except: pass
                        else:
                            logger.error(f"⛔ [失败] 放弃上传: {file_name}")
                            
                            current_time = time.time()
                            if rel_p not in failures_db:
                                failures_db[rel_p] = current_time
                                save_failures(failures_db)
                                logger.info(f"📝 [记录] 首次失败已记录")
                            else:
                                first_fail_time = failures_db[rel_p]
                                duration = current_time - first_fail_time
                                if duration > 86400:
                                    send_email(config, "严重：文件失败超24小时", f"文件卡死：\n{rel_p}")
                                    failures_db[rel_p] = current_time 
                                    save_failures(failures_db)
                                    logger.info(f"🚨 [报警] 失败超24小时，已邮件通知")
                                else:
                                    hours = duration / 3600
                                    logger.info(f"🔇 [静默] 已失败 {hours:.1f} 小时")

                    if folder_success_count > 0:
                        status_text = "本地已清理" if config.get('delete_after_upload', True) else "文件保留"
                        msg = f"目录：{folder_name}<br>成功：{folder_success_count} 个<br>状态：{status_text}"
                        send_email(config, "NAS文件夹任务完成", msg)
                        logger.info(f"🎉 [完成] 目录 {folder_name} 处理完毕")

                last_busy = time.time()
            else:
                if not is_idle_mode:
                    logger.info("💤 任务已完成，请继续添加文件...")
                    is_idle_mode = True
                
                now = time.time()
                # 检查开关
                if config.get('enable_idle_email', False):
                    if (now - last_busy) > safe_int(config.get('idle_interval'), 1800):
                        if (now - last_idle) > safe_int(config.get('idle_interval'), 1800):
                            send_email(config, "空闲提醒", "NAS空闲中，等待新任务")
                            last_idle = now
            
            for _ in range(5):
                if stop_event.is_set(): break
                time.sleep(1)
                
        except Exception as e:
            logger.error(f"⚠️ 系统错误: {e}")
            time.sleep(10)
    is_running = False
    logger.info("🛑 进程已停止")

@app.route('/')
def index():
    return render_template('index.html', config=load_config(), is_running=is_running)

@app.route('/help')
def help_page():
    return render_template('help.html')

@app.route('/save', methods=['POST'])
def save_settings():
    if is_running:
        return jsonify({"status": "error", "msg": "🚫 请先【停止服务】再保存！"})
    try:
        cfg = request.json
        if not cfg.get('hf_token'): return jsonify({"status": "error", "msg": "❌ HF Token 不能为空"})
        if not cfg.get('repo_id'): return jsonify({"status": "error", "msg": "❌ 仓库 ID 不能为空"})

        cfg['email_port'] = safe_int(cfg.get('email_port'), 465)
        cfg['warn_timeout'] = safe_int(cfg.get('warn_timeout'), 900)
        cfg['kill_timeout'] = safe_int(cfg.get('kill_timeout'), 1800)
        cfg['idle_interval'] = safe_int(cfg.get('idle_interval'), 1800)
        cfg['max_retries'] = safe_int(cfg.get('max_retries'), 3)
        cfg['notify_min_size'] = safe_int(cfg.get('notify_min_size'), 1024)
        cfg['file_interval'] = safe_int(cfg.get('file_interval'), 15)
        
        if save_config(cfg):
            return jsonify({"status": "success", "msg": "✅ 配置已保存！请点击启动"})
        else:
            return jsonify({"status": "error", "msg": "❌ 写入失败"})
    except Exception as e:
        return jsonify({"status": "error", "msg": f"❌ 错误: {str(e)}"})

@app.route('/reset', methods=['POST'])
def reset_settings():
    if is_running:
        return jsonify({"status": "error", "msg": "🚫 运行中无法重置，请先停止！"})
    try:
        if os.path.exists(CONFIG_FILE):
            os.remove(CONFIG_FILE)
        if os.path.exists(FAILURE_RECORD_FILE):
            os.remove(FAILURE_RECORD_FILE)
        return jsonify({"status": "success", "msg": "🗑️ 配置已清空！"})
    except Exception as e:
        return jsonify({"status": "error", "msg": f"❌ 重置失败: {str(e)}"})

@app.route('/start', methods=['POST'])
def start_worker():
    global uploader_thread, is_running, stop_event
    if is_running:
        return jsonify({"status": "warning", "msg": "⚠️ 服务已在运行"})
    
    cfg = load_config()
    stop_event.clear()
    uploader_thread = threading.Thread(target=uploader_daemon, args=(cfg,))
    uploader_thread.daemon = True
    uploader_thread.start()
    is_running = True
    return jsonify({"status": "success", "msg": "🚀 服务启动中..."})

@app.route('/stop', methods=['POST'])
def stop_worker():
    global stop_event
    stop_event.set()
    return jsonify({"status": "success", "msg": "🛑 正在停止..."})

@app.route('/logs')
def stream_logs():
    def generate():
        while True:
            if not LOG_QUEUE.empty():
                yield f"data: {LOG_QUEUE.get()}\n\n"
            else:
                time.sleep(0.5)
                yield f"data: \n\n"
    return Response(generate(), mimetype='text/event-stream')

if __name__ == '__main__':
    os.makedirs("/app/config", exist_ok=True)
    os.makedirs("/app/data", exist_ok=True)
    app.run(host='0.0.0.0', port=7860, debug=False, use_reloader=False, threaded=True)
