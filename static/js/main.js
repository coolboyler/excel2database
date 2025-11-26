// static/js/main.js

document.addEventListener('DOMContentLoaded', function() {
    // 初始化页面
    loadFileList();
    setupUploadForm();
    setupDragAndDrop();
});

// 加载文件列表
function loadFileList() {
    const fileListElement = document.getElementById('file-list');
    if (!fileListElement) return;

    // 设置固定高度和滚动条
    fileListElement.style.maxHeight = '200px';
    fileListElement.style.overflowY = 'auto';

    fileListElement.innerHTML = '<div class="text-center"><div class="spinner"></div> 加载中...</div>';
    
    fetch('/files')
        .then(response => response.json())
        .then(data => {
            if (data.total === 0) {
                fileListElement.innerHTML = '<div class="text-center">没有找到Excel文件，请上传文件。</div>';
                return;
            }
            
            // 限制最多显示5条数据
            const filesToShow = data.files.slice(0, 5);
            const hasMoreFiles = data.files.length > 5;
            
            let html = '';
            filesToShow.forEach(file => {
                html += `
                <li class="file-item">
                    <span class="file-name">${file}</span>
                    <div class="file-actions">
                        <button class="btn btn-primary btn-sm" onclick="importFile('${file}')">导入</button>
                        <button class="btn btn-danger btn-sm" onclick="deleteFile('${file}')">删除</button>
                    </div>
                </li>`;
            });
            
            if (hasMoreFiles) {
                html += `<div class="text-center">还有 ${data.files.length - 5} 个文件未显示</div>`;
            }
            
            fileListElement.innerHTML = html;
        })
        .catch(error => {
            console.error('Error loading file list:', error);
            fileListElement.innerHTML = '<div class="alert alert-danger">加载文件列表失败</div>';
        });
}

// 加载表列表
function loadTableList() {
    const tableListElement = document.getElementById('table-list');
    if (!tableListElement) return;

    // 设置固定高度和滚动条
    tableListElement.style.maxHeight = '200px';
    tableListElement.style.overflowY = 'auto';

    tableListElement.innerHTML = '<div class="text-center"><div class="spinner"></div> 加载中...</div>';
    
    fetch('/tables')
        .then(response => response.json())
        .then(data => {
            if (data.tables.length === 0) {
                tableListElement.innerHTML = '<div class="text-center">数据库中没有表。</div>';
                return;
            }
            
            // 限制最多显示5条数据
            const tablesToShow = data.tables.slice(0, 5);
            const hasMoreTables = data.tables.length > 5;
            
            let html = '';
            tablesToShow.forEach(table => {
                html += `
                <li class="file-item">
                    <span class="file-name">${table}</span>
                    <div class="file-actions">
                        <button class="btn btn-primary btn-sm" onclick="viewTableData('${table}')">查看数据</button>
                        <button class="btn btn-warning btn-sm" onclick="viewTableQuery('${table}')">查询</button>
                        <button class="btn btn-danger btn-sm" onclick="deleteTable('${table}')">删除表</button>
                    </div>
                </li>`;
            });
            
            if (hasMoreTables) {
                html += `<div class="text-center">还有 ${data.tables.length - 5} 个表未显示</div>`;
            }
            
            tableListElement.innerHTML = html;
        })
        .catch(error => {
            console.error('Error loading table list:', error);
            tableListElement.innerHTML = '<div class="alert alert-danger">加载表列表失败</div>';
        });
}

// 设置上传表单 - 支持批量上传
function setupUploadForm() {
    const uploadForm = document.getElementById('upload-form');
    const fileInput = document.getElementById('file-input');
    
    if (!uploadForm || !fileInput) {
        console.error('上传表单元素未找到');
        return;
    }
    
    const selectedFileDiv = document.getElementById('selected-file');
    const selectedFileNameSpan = document.getElementById('selected-file-name');
    const uploadButton = document.getElementById('upload-button');
    
    // 修改文件输入框为多选
    fileInput.multiple = true;
    
    // 监听文件选择事件
    fileInput.addEventListener('change', function(e) {
        console.log('文件选择事件触发', e.target.files);
        const files = Array.from(e.target.files);
        
        if (files.length > 0) {
            // 检查文件类型
            const validFiles = files.filter(file => file.name.endsWith('.xlsx'));
            const invalidFiles = files.filter(file => !file.name.endsWith('.xlsx'));
            
            if (invalidFiles.length > 0) {
                const invalidFileNames = invalidFiles.map(f => f.name).join(', ');
                showAlert(`以下文件不是.xlsx格式，将被忽略: ${invalidFileNames}`, 'warning');
            }
            
            if (validFiles.length > 0) {
                console.log('选择的有效文件:', validFiles.map(f => f.name));
                if (selectedFileDiv && selectedFileNameSpan) {
                    selectedFileNameSpan.textContent = `${validFiles.length} 个文件`;
                    selectedFileDiv.style.display = 'block';
                    console.log('显示文件选择区域');
                }
            } else {
                console.error('没有有效的文件');
                showAlert('请选择至少一个.xlsx格式的Excel文件', 'danger');
                if (selectedFileDiv) {
                    selectedFileDiv.style.display = 'none';
                }
                fileInput.value = '';
            }
        } else {
            console.log('用户取消了文件选择');
            if (selectedFileDiv) {
                selectedFileDiv.style.display = 'none';
            }
        }
    });
    
    // 监听上传按钮点击事件
    if (uploadButton) {
        uploadButton.addEventListener('click', function(e) {
            e.preventDefault(); // 防止表单提交
            console.log('上传按钮被点击');
            
            const files = Array.from(fileInput.files);
            const validFiles = files.filter(file => file.name.endsWith('.xlsx'));
            
            if (validFiles.length > 0) {
                console.log('开始上传文件:', validFiles.map(f => f.name));
                uploadFiles(validFiles);
            } else {
                showAlert('请选择至少一个有效的Excel文件 (.xlsx)', 'danger');
            }
        });
    }
    
    // 表单提交事件处理
    uploadForm.addEventListener('submit', function(e) {
        e.preventDefault();
        console.log('表单提交事件');
        
        const files = Array.from(fileInput.files);
        const validFiles = files.filter(file => file.name.endsWith('.xlsx'));
        
        if (validFiles.length > 0) {
            uploadFiles(validFiles);
        } else {
            showAlert('请选择至少一个有效的Excel文件 (.xlsx)', 'danger');
        }
    });
}

// 设置拖放上传 - 支持批量上传
function setupDragAndDrop() {
    const dropArea = document.getElementById('drop-area');
    if (!dropArea) return;
    
    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
        dropArea.addEventListener(eventName, preventDefaults, false);
    });
    
    function preventDefaults(e) {
        e.preventDefault();
        e.stopPropagation();
    }
    
    ['dragenter', 'dragover'].forEach(eventName => {
        dropArea.addEventListener(eventName, () => {
            dropArea.classList.add('dragover');
        }, false);
    });
    
    ['dragleave', 'drop'].forEach(eventName => {
        dropArea.addEventListener(eventName, () => {
            dropArea.classList.remove('dragover');
        }, false);
    });
    
    dropArea.addEventListener('drop', handleDrop, false);
    
    function handleDrop(e) {
        const dt = e.dataTransfer;
        const files = Array.from(dt.files);
        const validFiles = files.filter(file => file.name.endsWith('.xlsx'));
        const invalidFiles = files.filter(file => !file.name.endsWith('.xlsx'));
        
        if (invalidFiles.length > 0) {
            const invalidFileNames = invalidFiles.map(f => f.name).join(', ');
            showAlert(`以下文件不是.xlsx格式，将被忽略: ${invalidFileNames}`, 'warning');
        }
        
        if (validFiles.length > 0) {
            uploadFiles(validFiles);
        } else {
            showAlert('请至少上传一个.xlsx格式的Excel文件', 'danger');
        }
    }
}

// 上传多个文件 - 支持重复文件检测
async function uploadFiles(files) {
    console.log('开始上传文件:', files.map(f => f.name));
    
    // 获取当前文件列表
    let currentFiles = [];
    try {
        const response = await fetch('/files');
        const data = await response.json();
        currentFiles = data.files || [];
    } catch (error) {
        console.error('获取当前文件列表失败:', error);
    }
    
    // 检查是否有重复文件
    const duplicateFiles = files.filter(file => currentFiles.includes(file.name));
    const newFiles = files.filter(file => !currentFiles.includes(file.name));
    
    let filesToUpload = [...files]; // 默认上传所有文件
    
    if (duplicateFiles.length > 0) {
        const duplicateFileNames = duplicateFiles.map(f => f.name).join(', ');
        const message = `以下文件已存在: ${duplicateFileNames}\n是否要覆盖这些文件?`;
        
        if (!confirm(message)) {
            // 用户选择不覆盖，只上传新文件
            filesToUpload = newFiles;
            if (filesToUpload.length === 0) {
                showAlert('没有需要上传的新文件', 'info');
                return;
            }
        }
    }
    
    // 逐个上传文件
    const progressBar = document.getElementById('progress-bar');
    const progressContainer = document.getElementById('progress-container');
    
    // 显示进度条
    if (progressContainer) {
        progressContainer.classList.remove('hidden');
    }
    
    let successCount = 0;
    let failCount = 0;
    
    for (let i = 0; i < filesToUpload.length; i++) {
        const file = filesToUpload[i];
        const progress = Math.round(((i + 1) / filesToUpload.length) * 100);
        
        if (progressBar) {
            progressBar.style.width = `${progress}%`;
        }
        
        try {
            await uploadSingleFile(file);
            successCount++;
        } catch (error) {
            console.error(`上传文件 ${file.name} 失败:`, error);
            failCount++;
        }
    }
    
    // 完成后隐藏进度条
    setTimeout(() => {
        if (progressContainer) {
            progressContainer.classList.add('hidden');
        }
        if (progressBar) {
            progressBar.style.width = '0%';
        }
    }, 1000);
    
    // 显示结果
    if (failCount === 0) {
        showAlert(`成功上传 ${successCount} 个文件`, 'success');
    } else {
        showAlert(`上传完成: ${successCount} 个成功, ${failCount} 个失败`, failCount > 0 ? 'warning' : 'success');
    }
    
    // 重置文件输入框
    const fileInput = document.getElementById('file-input');
    const selectedFileDiv = document.getElementById('selected-file');
    
    if (fileInput) {
        fileInput.value = '';
    }
    if (selectedFileDiv) {
        selectedFileDiv.style.display = 'none';
    }
    
    // 刷新文件列表
    loadFileList();
}

// 上传单个文件
function uploadSingleFile(file) {
    return new Promise((resolve, reject) => {
        console.log('开始上传单个文件:', file.name);
        
        const formData = new FormData();
        formData.append('file', file);
        
        fetch('/upload', {
            method: 'POST',
            body: formData
        })
        .then(response => {
            if (!response.ok) {
                throw new Error('上传失败，服务器返回错误状态: ' + response.status);
            }
            return response.json();
        })
        .then(data => {
            console.log('上传成功:', data);
            resolve(data);
        })
        .catch(error => {
            console.error('上传文件失败:', error);
            reject(error);
        });
    });
}

// 导入文件
function importFile(filename) {
    const formData = new FormData();
    formData.append('filename', filename);
    
    // 修复选择器：使用标准DOM方法查找包含特定文件名的元素
    const fileItems = document.querySelectorAll('.file-item');
    let fileItem = null;
    fileItems.forEach(item => {
        const spanElement = item.querySelector('.file-name');
        if (spanElement && spanElement.textContent === filename) {
            fileItem = item;
        }
    });
    
    if (fileItem) {
        const actionArea = fileItem.querySelector('.file-actions');
        actionArea.innerHTML = '<div class="spinner"></div> 导入中...';
    }
    
    fetch('/import', {
        method: 'POST',
        body: formData
    })
    .then(response => {
        if (!response.ok) {
            throw new Error('导入失败');
        }
        return response.json();
    })
    .then(data => {
        // 添加调试信息
        console.log('Import response data:', data);
        
        if (data && data.status === 'imported') {
            showAlert(`文件 ${filename} 导入成功`, 'success');
        } else {
            showAlert(`文件 ${filename} 导入处理中`, 'success');
        }
        
        // 使用相同的方法查找文件项
        const fileItems = document.querySelectorAll('.file-item');
        let fileItem = null;
        fileItems.forEach(item => {
            const spanElement = item.querySelector('.file-name');
            if (spanElement && spanElement.textContent === filename) {
                fileItem = item;
            }
        });
        
        if (fileItem) {
            const actionArea = fileItem.querySelector('.file-actions');
            
            // 创建预览数据表格
            let previewHtml = '';
            if (data && data.preview_data && data.preview_data.length > 0) {
                previewHtml = '<div class="data-preview"><h4>数据预览</h4><div class="table-container"><table class="preview-table"><thead><tr>';
                
                // 表头 - 添加标题提示
                const headers = Object.keys(data.preview_data[0]);
                headers.forEach(header => {
                    previewHtml += `<th title="${header}">${header}</th>`;
                });
                previewHtml += '</tr></thead><tbody>';
                
                // 表格数据 - 添加标题提示
                data.preview_data.forEach(row => {
                    previewHtml += '<tr>';
                    headers.forEach(header => {
                        let value = row[header] !== null ? row[header] : '';
                        // 在这里加上时间转换
                        if (header === "record_time" && value !== "") {
                            console.log("record_time 原始值:", value, "类型:", typeof value);
                            
                            // 处理字符串类型
                            const numValue = parseInt(value);
                            console.log("转换为数字:", numValue);
                            
                            // 将秒数转换为分钟，再格式化为时间
                            const totalSeconds = numValue;
                            const totalMinutes = totalSeconds / 60; // 秒转分钟
                            const hours = Math.floor(totalMinutes / 60) % 24;
                            const minutes = Math.floor(totalMinutes % 60);
                            value = `${hours.toString().padStart(2, "0")}:${minutes.toString().padStart(2, "0")}`;
                            
                            console.log("转换后:", value);
                        }
                        // 为单元格添加title属性以便查看完整内容
                        previewHtml += `<td title="${value}">${value}</td>`;
                    });
                    previewHtml += '</tr>';
                });

                previewHtml += '</tbody></table></div></div>';
            }
            
            // 确保使用正确的数据字段（修复关键问题）
            const tableName = (data && data.table_name) ? data.table_name : 'unknown';
            const recordCount = (data && data.record_count) ? data.record_count : 0;
            
            actionArea.innerHTML = `
                <span class="status-badge status-success">已导入</span>
                <div class="import-info">导入到表: <a href="#" onclick="viewTableData('${tableName}')">${tableName}</a><br>共 ${recordCount} 条记录</div>
                <div class="btn-group">
                    <button class="btn btn-primary btn-sm" onclick="importFile('${filename}')">重新导入</button>
                    <button class="btn btn-danger btn-sm" onclick="deleteFile('${filename}')">删除</button>
                    <button class="btn btn-warning btn-sm" onclick="deleteTable('${tableName}')">删除表</button>
                </div>
                ${previewHtml}
            `;
        }
        
        // 刷新表列表
        loadTableList();
    })
    .catch(error => {
        console.error('Error importing file:', error);
        showAlert('导入文件失败: ' + error.message, 'danger');
        
        // 使用相同的方法查找文件项
        const fileItems = document.querySelectorAll('.file-item');
        let fileItem = null;
        fileItems.forEach(item => {
            const spanElement = item.querySelector('.file-name');
            if (spanElement && spanElement.textContent === filename) {
                fileItem = item;
            }
        });
        
        if (fileItem) {
            const actionArea = fileItem.querySelector('.file-actions');
            actionArea.innerHTML = `
                <span class="status-badge status-error">导入失败</span>
                <div class="btn-group">
                    <button class="btn btn-primary btn-sm" onclick="importFile('${filename}')">重试</button>
                    <button class="btn btn-danger btn-sm" onclick="deleteFile('${filename}')">删除</button>
                </div>
            `;
        }
    });
}

// 删除文件
function deleteFile(filename) {
    if (!confirm(`确定要删除文件 ${filename} 吗？`)) {
        return;
    }
    
    fetch(`/files/${filename}`, {
        method: 'DELETE'
    })
    .then(response => {
        if (!response.ok) {
            throw new Error('删除失败');
        }
        return response.json();
    })
    .then(data => {
        showAlert(`文件 ${data.filename} 删除成功`, 'success');
        loadFileList();
    })
    .catch(error => {
        console.error('Error deleting file:', error);
        showAlert('删除文件失败', 'danger');
    });
}

// 导入所有文件
function importAllFiles() {
    const importAllBtn = document.getElementById('import-all-btn');
    importAllBtn.disabled = true;
    importAllBtn.innerHTML = '<div class="spinner"></div> 导入中...';
    
    fetch('/import-all', {
        method: 'POST'
    })
    .then(response => {
        if (!response.ok) {
            throw new Error('批量导入失败');
        }
        return response.json();
    })
    .then(data => {
        showAlert(`开始导入 ${data.total} 个文件`, 'success');
        
        // 更新所有文件状态
        const fileItems = document.querySelectorAll('.file-item');
        fileItems.forEach(item => {
            const actionArea = item.querySelector('.file-actions');
            const filename = item.querySelector('.file-name').textContent;
            actionArea.innerHTML = `
                <span class="status-badge status-processing">导入中</span>
                <button class="btn btn-danger btn-sm" onclick="deleteFile('${filename}')">删除</button>
            `;
        });
        
        // 启动状态检查定时器
        checkImportStatus();
        
        importAllBtn.disabled = false;
        importAllBtn.innerHTML = '导入所有文件';
    })
    .catch(error => {
        console.error('Error importing all files:', error);
        showAlert('批量导入文件失败', 'danger');
        
        importAllBtn.disabled = false;
        importAllBtn.innerHTML = '导入所有文件';
    });
}

// 检查导入状态
function checkImportStatus() {
    // 每隔5秒检查一次文件列表状态
    const interval = setInterval(() => {
        fetch('/files')
            .then(response => response.json())
            .then(data => {
                // 检查是否还有文件处于"导入中"状态
                const processingItems = document.querySelectorAll('.status-processing');
                if (processingItems.length === 0) {
                    // 如果没有处理中的项目，停止检查
                    clearInterval(interval);
                    return;
                }
                
                // 重新加载文件列表以更新状态
                loadFileList();
                
                // 检查是否所有文件都已处理完成
                setTimeout(() => {
                    const processingItems = document.querySelectorAll('.status-processing');
                    if (processingItems.length === 0) {
                        clearInterval(interval);
                        showAlert('所有文件导入完成', 'success');
                        // 刷新列表以显示导入成功状态
                        setTimeout(() => {
                            loadFileList();
                            loadTableList(); // 同时刷新表列表
                                    
                            // 重置导入所有按钮状态
                            const importAllBtn = document.getElementById('import-all-btn');
                            if (importAllBtn) {
                                importAllBtn.disabled = false;
                                importAllBtn.innerHTML = '已导入';
                            }
                        }, 1000);
                    }
                }, 1000);
            })
            .catch(error => {
                console.error('Error checking import status:', error);
            });
    }, 5000); // 每5秒检查一次
}

// 查看表数据
function viewTableData(tableName) {
    // 添加检查确保tableName有效
    if (!tableName || tableName === 'unknown') {
        showAlert('无效的表名', 'danger');
        return;
    }
    
    // 创建模态框
    const modal = document.createElement('div');
    modal.className = 'modal';
    modal.innerHTML = `
        <div class="modal-content">
            <div class="modal-header">
                <h3>表数据: ${tableName}</h3>
                <span class="close-modal">&times;</span>
            </div>
            <div class="modal-body">
                <div class="text-center"><div class="spinner"></div> 加载中...</div>
            </div>
            <div class="modal-footer">
                <button class="btn btn-danger" onclick="deleteTable('${tableName}')">删除表</button>
                <button class="btn btn-secondary close-btn">关闭</button>
            </div>
        </div>
    `;
    document.body.appendChild(modal);
    
    // 确保模态框在视口中可见
    modal.style.display = 'block';
    
    // 关闭模态框事件
    modal.querySelector('.close-modal').addEventListener('click', () => {
        document.body.removeChild(modal);
    });
    
    modal.querySelector('.close-btn').addEventListener('click', () => {
        document.body.removeChild(modal);
    });
    
    // 点击模态框背景关闭
    modal.addEventListener('click', (e) => {
        if (e.target === modal) {
            document.body.removeChild(modal);
        }
    });
    
    // 按ESC键关闭模态框
    document.addEventListener('keydown', function escapeHandler(e) {
        if (e.key === 'Escape') {
            if (document.body.contains(modal)) {
                document.body.removeChild(modal);
            }
            document.removeEventListener('keydown', escapeHandler);
        }
    });
    
    // 加载表数据
    fetch(`/tables/${tableName}`)
        .then(response => response.json())
        .then(result => {
            const modalBody = modal.querySelector('.modal-body');
            
            if (result.data && result.data.length > 0) {
                let tableHtml = `<div class="table-info">总记录数: ${result.total}</div>`;
                tableHtml += '<div class="table-container" style="max-height: 500px; overflow-y: auto;">';
                tableHtml += '<table class="data-table"><thead><tr>';
                
                // 表头
                const headers = Object.keys(result.data[0]);
                headers.forEach(header => {
                    tableHtml += `<th>${header}</th>`;
                });
                tableHtml += '</tr></thead><tbody>';
                
                // 表格数据
                result.data.forEach(row => {
                    tableHtml += '<tr>';
                    headers.forEach(header => {
                        let value = row[header] !== null ? row[header] : '';
                        // 特别处理 record_time 字段
                        if (header === "record_time" && value !== "") {
                            // 将秒数转换为分钟，再格式化为时间
                            const totalSeconds = parseInt(value);
                            const totalMinutes = totalSeconds / 60; // 秒转分钟
                            const hours = Math.floor(totalMinutes / 60) % 24;
                            const minutes = Math.floor(totalMinutes % 60);
                            value = `${hours.toString().padStart(2, "0")}:${minutes
                              .toString()
                              .padStart(2, "0")}`;
                          }
                        tableHtml += `<td>${value}</td>`;
                    });
                    tableHtml += '</tr>';
                });
                
                tableHtml += '</tbody></table>';
                tableHtml += '</div>';
                modalBody.innerHTML = tableHtml;
            } else {
                modalBody.innerHTML = '<div class="alert alert-info">表中没有数据</div>';
            }
        })
        .catch(error => {
            console.error('Error loading table data:', error);
            const modalBody = modal.querySelector('.modal-body');
            modalBody.innerHTML = '<div class="alert alert-danger">加载表数据失败: ' + error.message + '</div>';
        });
}

// 查看表查询页面
function viewTableQuery(tableName) {
    // 跳转到查询页面
    window.open(`/table_query?table_name=${encodeURIComponent(tableName)}`, '_blank');
}

// 删除表
function deleteTable(tableName) {
    if (!confirm(`确定要删除表 ${tableName} 吗？此操作不可恢复！`)) {
        return;
    }
    
    fetch(`/tables/${tableName}`, {
        method: 'DELETE'
    })
    .then(response => response.json())
    .then(data => {
        showAlert(`表 ${tableName} 已成功删除`, 'success');
        
        // 关闭可能打开的模态框
        const modal = document.querySelector('.modal');
        if (modal) {
            document.body.removeChild(modal);
        }
        
        // 更新文件列表，以便更新状态
        loadFileList();
        
        // 更新表列表
        loadTableList();
    })
    .catch(error => {
        console.error('Error deleting table:', error);
        showAlert(`删除表 ${tableName} 失败`, 'danger');
    });
}

// 显示提示信息
function showAlert(message, type) {
    const alertsContainer = document.getElementById('alerts');
    if (!alertsContainer) return;
    
    const alert = document.createElement('div');
    alert.className = `alert alert-${type}`;
    alert.textContent = message;
    
    alertsContainer.appendChild(alert);
    
    // 3秒后自动消失
    setTimeout(() => {
        alert.style.opacity = '0';
        setTimeout(() => {
            alertsContainer.removeChild(alert);
        }, 300);
    }, 3000);
}