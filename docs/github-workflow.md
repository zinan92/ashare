# GitHub项目管控工作流程

## 快速开始

### 1. 连接GitHub

```bash
# 替换YOUR_USERNAME为你的GitHub用户名
git remote add origin https://github.com/YOUR_USERNAME/a-share-monitor.git
git branch -M main
git push -u origin main
```

### 2. 日常开发

```bash
# 创建功能分支
git checkout -b feature/your-feature

# 开发完成后提交
git add .
git commit -m "feat: 添加新功能"
git push origin feature/your-feature
```

### 3. 提交规范

- `feat`: 新功能
- `fix`: Bug修复  
- `docs`: 文档
- `perf`: 性能优化

## 详细文档

完整的GitHub工作流程请参考项目Wiki。
