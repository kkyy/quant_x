import os
import shutil
import tarfile
import urllib.request
import requests
from typing import Optional


def download_and_extract_qlib_data(
    url: str = "https://github.com/chenditc/investment_data/releases/latest/download/qlib_bin.tar.gz",
    save_path: str = "/tmp/qlib_bin.tar.gz",
    data_path: str = "/tmp/qlib_data/cn_data",
    force_redownload: bool = True,
    force_refextract: bool = True,
    use_requests: bool = True,
) -> None:
    """
    下载并解压 Qlib 数据包，自动去除顶层目录（类似 --strip-components=1）

    参数:
        url (str): 数据包下载链接
        save_path (str): 下载后的本地保存路径
        data_path (str): 解压目标路径
        force_redownload (bool): 是否强制重新下载（如果文件已存在）
        force_refextract (bool): 是否强制重新解压（如果目标目录已存在）
        use_requests (bool): 是否使用 requests 库下载（需提前安装），否则使用 urllib
    """
    tmp_dir = os.path.dirname(save_path)
    os.makedirs(tmp_dir, exist_ok=True)

    # 1. 下载文件
    if force_redownload or not os.path.exists(save_path):
        print(f"Downloading from {url} to {save_path}...")
        try:
            if use_requests:
                import requests as req_lib
                with req_lib.get(url, stream=True, timeout=30) as r:
                    r.raise_for_status()
                    with open(save_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
            else:
                urllib.request.urlretrieve(url, save_path)
            print("Download completed.")
        except Exception as e:
            if os.path.exists(save_path):
                os.remove(save_path)
            raise RuntimeError(f"Download failed: {e}")
    else:
        print(f"Using existing download: {save_path}")

    # 2. 删除并创建目标数据目录
    if force_refextract or os.path.exists(data_path):
        if os.path.exists(data_path):
            print(f"Removing existing data directory: {data_path}")
            shutil.rmtree(data_path)
        os.makedirs(data_path, exist_ok=True)
        print(f"Created data directory: {data_path}")

        # 3. 解压并 strip 第一层目录
        print(f"Extracting {save_path} to {data_path} (stripping top-level directory)...")
        try:
            with tarfile.open(save_path, 'r:gz') as tar:
                # 获取所有成员
                members = tar.getmembers()

                if not members:
                    print("Warning: Archive is empty.")
                    return

                # 获取顶层目录名（如 qlib_bin）
                top_level_prefix = members[0].name.split('/')[0]

                def _strip_top_level(member):
                    """修改归档成员路径，去掉第一级目录"""
                    parts = member.name.split('/')
                    if len(parts) > 1:
                        member.name = '/'.join(parts[1:])
                        return member
                    else:
                        return None  # 忽略根文件/目录

                # 过滤并重映射路径
                stripped_members = []
                for member in members:
                    new_member = _strip_top_level(member)
                    if new_member is not None:
                        stripped_members.append(new_member)

                tar.extractall(path=data_path, members=stripped_members)
            print("Extraction completed.")
        except Exception as e:
            raise RuntimeError(f"Extraction failed: {e}")
    else:
        print(f"Skipped extraction: data already exists at {data_path}")

    # 4. 清理下载文件（可选：注释掉以保留缓存）
    if os.path.exists(save_path):
        os.remove(save_path)
        print(f"Cleaned up downloaded archive: {save_path}")

    print(f"✅ Successfully downloaded and extracted Qlib data to: {data_path}")


# =====================================
# 使用示例
# =====================================
if __name__ == "__main__":
    # 示例1：下载 A 股数据
    download_and_extract_qlib_data(
        url="https://github.com/chenditc/investment_data/releases/latest/download/qlib_bin.tar.gz",
        save_path="/tmp/qlib_bin.tar.gz",
        data_path="/tmp/qlib_data/cn_data"
    )

    # 示例2：可以调用另一个数据集（如果有）
    # download_and_extract_qlib_data(
    #     url="https://example.com/us_data.tar.gz",
    #     save_path="/tmp/us_data.tar.gz",
    #     data_path="qlib_data/us_data"
    # )