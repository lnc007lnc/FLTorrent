#!/usr/bin/env python3
"""
手动处理 CelebA 数据，使用全部数据 (s_frac=1.0)
"""
import os
import json
import torch
import numpy as np
from tqdm import tqdm
from sklearn.model_selection import train_test_split

# 配置
raw_path = "data/celeba/raw/all_data"
processed_dir = "data/celeba/processed"
s_frac = 0.25  # 使用 25% 数据
tr_frac = 0.8  # 训练集比例
val_frac = 0.1  # 验证集比例
seed = 1234

# 清空并重建 processed 目录
import shutil
if os.path.exists(processed_dir):
    shutil.rmtree(processed_dir)
os.makedirs(processed_dir, exist_ok=True)

# 获取所有 JSON 文件
files = [f for f in os.listdir(raw_path) if f.endswith('.json')]
print(f"找到 {len(files)} 个 JSON 文件")

n_tasks = int(np.ceil(len(files) * s_frac))
print(f"将处理 {n_tasks} 个文件 (s_frac={s_frac})")

# 处理所有文件
idx = 0
total_train = 0
total_val = 0
total_test = 0

for num, file in enumerate(tqdm(files[:n_tasks], desc="处理文件")):
    try:
        with open(os.path.join(raw_path, file), 'r') as f:
            raw_data = json.load(f)

        # 处理每个用户
        for writer, v in raw_data['user_data'].items():
            data, targets = v['x'], v['y']

            if len(data) < 3:  # 跳过样本太少的用户
                continue

            # 转换为 Tensor
            data = torch.tensor(np.stack(data))
            targets = torch.LongTensor(np.stack(targets))

            # 分割训练/测试集
            train_data, test_data, train_targets, test_targets = train_test_split(
                data, targets, train_size=tr_frac, random_state=seed
            )

            # 分割验证集
            if val_frac > 0 and len(test_data) >= 2:
                val_data, test_data, val_targets, test_targets = train_test_split(
                    test_data, test_targets,
                    train_size=val_frac / (1. - tr_frac),
                    random_state=seed
                )
            else:
                val_data, val_targets = None, None

            # 保存
            save_path = os.path.join(processed_dir, f"task_{idx}")
            os.makedirs(save_path, exist_ok=True)

            torch.save((train_data, train_targets), os.path.join(save_path, 'train.pt'))
            torch.save((test_data, test_targets), os.path.join(save_path, 'test.pt'))
            if val_data is not None:
                torch.save((val_data, val_targets), os.path.join(save_path, 'val.pt'))

            total_train += len(train_targets)
            total_test += len(test_targets)
            if val_targets is not None:
                total_val += len(val_targets)

            idx += 1

    except Exception as e:
        print(f"处理文件 {file} 出错: {e}")
        continue

print(f"\n处理完成!")
print(f"总共生成 {idx} 个 task (用户)")
print(f"训练样本: {total_train}")
print(f"验证样本: {total_val}")
print(f"测试样本: {total_test}")
print(f"总样本数: {total_train + total_val + total_test}")
