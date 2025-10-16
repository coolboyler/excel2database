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
    
    fileListElement.innerHTML = '<div class="text-center"><div class="spinner"></div> 加载中...</div>';
    
    fetch('/files')
        .then(response => response.json())
        .then(data => {
            if (data.total === 0) {
                fileListElement.innerHTML = '<div class="text-center">没有找到Excel文件，请上传文件。</div>';
                return;
            }
            
            let html = '';
            data.files.forEach(file => {
                html += `
                <li class="file-item">
                    <span class="file-name">${file}</span>
                    <div class="file-actions">
                        <button class="btn btn-primary btn-sm" onclick="importFile('${file}')">导入</button>
                        <button class="btn btn-danger btn-sm" onclick="deleteFile('${file}')">删除</button>
                    </div>
                </li>`;
            });
            
            fileListElement.innerHTML = html;
        })
        .catch(error => {
            console.error('Error loading file list:', error);
            fileListElement.innerHTML = '<div class="alert alert-danger">加载文件列表失败</div>';
        });
}

// 设置上传表单
function setupUploadForm() {
    const uploadForm = document.getElementById('upload-form');
    if (!uploadForm) return;
    
    uploadForm.addEventListener('submit', function(e) {
        e.preventDefault();
        
        const fileInput = document.getElementById('file-input');
        const file = fileInput.files[0];
        
        if (!file) {
            showAlert('请选择一个Excel文件', 'danger');
            return;
        }
        
        uploadFile(file);
    });
}

// 设置拖放上传
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
        const file = dt.files[0];
        
        if (file && file.name.endsWith('.xlsx')) {
            uploadFile(file);
        } else {
            showAlert('请上传.xlsx格式的Excel文件', 'danger');
        }
    }
}

// 上传文件
function uploadFile(file) {
    const formData = new FormData();
    formData.append('file', file);
    
    const progressBar = document.getElementById('progress-bar');
    const progressContainer = document.getElementById('progress-container');
    
    progressContainer.classList.remove('hidden');
    progressBar.style.width = '0%';
    
    fetch('/upload', {
        method: 'POST',
        body: formData
    })
    .then(response => {
        progressBar.style.width = '100%';
        
        if (!response.ok) {
            throw new Error('上传失败');
        }
        return response.json();
    })
    .then(data => {
        showAlert(`文件 ${data.filename} 上传成功`, 'success');
        loadFileList();
        
        // 重置表单
        document.getElementById('upload-form').reset();
        
        setTimeout(() => {
            progressContainer.classList.add('hidden');
            progressBar.style.width = '0%';
        }, 1000);
    })
    .catch(error => {
        console.error('Error uploading file:', error);
        showAlert('上传文件失败', 'danger');
        progressContainer.classList.add('hidden');
    });
}

// 导入文件
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
                previewHtml = '<div class="data-preview"><h4>数据预览</h4><table class="preview-table"><thead><tr>';
                
                // 表头
                const headers = Object.keys(data.preview_data[0]);
                headers.forEach(header => {
                    previewHtml += `<th>${header}</th>`;
                });
                previewHtml += '</tr></thead><tbody>';
                
                // 表格数据
                data.preview_data.forEach(row => {
                    previewHtml += '<tr>';
                    headers.forEach(header => {
                        previewHtml += `<td>${row[header] !== null ? row[header] : ''}</td>`;
                    });
                    previewHtml += '</tr>';
                });
                
                previewHtml += '</tbody></table></div>';
            }
            
            // 确保使用正确的数据字段（修复关键问题）
            const tableName = (data && data.table_name) ? data.table_name : 'unknown';
            const recordCount = (data && data.record_count) ? data.record_count : 0;
            
            actionArea.innerHTML = `
                <span class="status-badge status-success">导入成功</span>
                <div class="import-info">导入到表: <a href="#" onclick="viewTableData('${tableName}')">${tableName}</a><br>共 ${recordCount} 条记录</div>
                <div class="btn-group">
                    <button class="btn btn-primary btn-sm" onclick="importFile('${filename}')">重新导入</button>
                    <button class="btn btn-danger btn-sm" onclick="deleteFile('${filename}')">删除</button>
                    <button class="btn btn-warning btn-sm" onclick="deleteTable('${tableName}')">删除表</button>
                </div>
                ${previewHtml}
            `;
        }
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
    
    // 关闭模态框事件
    modal.querySelector('.close-modal').addEventListener('click', () => {
        document.body.removeChild(modal);
    });
    modal.querySelector('.close-btn').addEventListener('click', () => {
        document.body.removeChild(modal);
    });
    
    // 加载表数据
    fetch(`/tables/${tableName}`)
        .then(response => response.json())
        .then(result => {
            const modalBody = modal.querySelector('.modal-body');
            
            if (result.data && result.data.length > 0) {
                let tableHtml = `<div class="table-info">总记录数: ${result.total}</div>`;
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
                        // static/js/main.js

                        // 在 viewTableData 函数中，找到处理 record_time 的部分并替换为：

                        // 特别处理 record_time 字段
                        if (header === 'record_time' && value !== '') {
                            // 如果是数字格式的时间（如 900 -> 00:15, 1800 -> 18:00）
                            if (typeof value === 'number') {
                                const hours = Math.floor(value / 100);
                                const minutes = value % 100;
                                value = `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}`;
                            }
                            // 如果是字符串格式的数字
                            else if (typeof value === 'string' && !isNaN(value)) {
                                const numValue = parseInt(value);
                                const hours = Math.floor(numValue / 100);
                                const minutes = numValue % 100;
                                value = `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}`;
                            }
                            // 如果是时间对象格式
                            else if (value instanceof Date) {
                                const hours = value.getHours();
                                const minutes = value.getMinutes();
                                value = `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}`;
                            }
                        }
                        tableHtml += `<td>${value}</td>`;
                    });
                    tableHtml += '</tr>';
                });
                
                tableHtml += '</tbody></table>';
                modalBody.innerHTML = tableHtml;
            } else {
                modalBody.innerHTML = '<div class="alert alert-info">表中没有数据</div>';
            }
        })
        .catch(error => {
            console.error('Error loading table data:', error);
            modal.querySelector('.modal-body').innerHTML = '<div class="alert alert-danger">加载表数据失败: ' + error.message + '</div>';
        });
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