import os
import torch

processed_dir = "/app/data/celeba/processed"
tasks = [d for d in os.listdir(processed_dir) if d.startswith("task_")]

# 采样几个 task 来估算平均大小
sample_tasks = tasks[:100]

total_train_size = 0
total_val_size = 0
total_test_size = 0
total_train_samples = 0
total_val_samples = 0
total_test_samples = 0

for task in sample_tasks:
    task_path = os.path.join(processed_dir, task)

    train_path = os.path.join(task_path, "train.pt")
    if os.path.exists(train_path):
        train_data, train_targets = torch.load(train_path, weights_only=False)
        total_train_size += train_data.element_size() * train_data.nelement()
        total_train_size += train_targets.element_size() * train_targets.nelement()
        total_train_samples += len(train_targets)

    val_path = os.path.join(task_path, "val.pt")
    if os.path.exists(val_path):
        val_data, val_targets = torch.load(val_path, weights_only=False)
        total_val_size += val_data.element_size() * val_data.nelement()
        total_val_size += val_targets.element_size() * val_targets.nelement()
        total_val_samples += len(val_targets)

    test_path = os.path.join(task_path, "test.pt")
    if os.path.exists(test_path):
        test_data, test_targets = torch.load(test_path, weights_only=False)
        total_test_size += test_data.element_size() * test_data.nelement()
        total_test_size += test_targets.element_size() * test_targets.nelement()
        total_test_samples += len(test_targets)

num_tasks = len(tasks)
scale = num_tasks / len(sample_tasks)

print(f"=== CelebA 数据内存估算 ===")
print(f"总 task 数量: {num_tasks}")
print(f"采样 task 数量: {len(sample_tasks)}")
print()
print(f"--- 采样数据统计 ---")
print(f"Train: {total_train_samples} 样本, {total_train_size / 1024 / 1024:.2f} MB")
print(f"Val:   {total_val_samples} 样本, {total_val_size / 1024 / 1024:.2f} MB")
print(f"Test:  {total_test_samples} 样本, {total_test_size / 1024 / 1024:.2f} MB")
print(f"合计:  {(total_train_size + total_val_size + total_test_size) / 1024 / 1024:.2f} MB")
print()

est_total = (total_train_size + total_val_size + total_test_size) * scale
print(f"--- 全量数据估算 (x{scale:.1f}) ---")
print(f"Tensor 原始大小: {est_total / 1024 / 1024 / 1024:.2f} GB")
print(f"考虑 PyTorch 开销 (x1.5): {est_total * 1.5 / 1024 / 1024 / 1024:.2f} GB")
print(f"考虑 data_dict + 拷贝 (x2.5): {est_total * 2.5 / 1024 / 1024 / 1024:.2f} GB")
print()

print(f"--- 不同 DATA_SUBSAMPLE 内存需求 ---")
for ratio in [1.0, 0.5, 0.3, 0.2, 0.1]:
    mem = est_total * 2.5 * ratio / 1024 / 1024 / 1024
    print(f"  subsample={ratio}: ~{mem:.2f} GB")
