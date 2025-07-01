#!/usr/bin/env python3
import os
import glob
from pathlib import Path
import json

def find_contamination_files(search_directory, output_file=None):
    """
    指定ディレクトリから contamination 関連ファイルを検索してパスリストを作成
    
    Args:
        search_directory (str): 検索対象のディレクトリパス
        output_file (str, optional): 結果を保存するファイルパス
    
    Returns:
        dict: ファイルタイプ別のパスリスト
    """
    
    # 検索パターンの定義
    patterns = {
        'getpileupsummaries': '*_getpileupsummaries.table',
        'calculatecontamination': '*_calculatecontamination.table', 
        'segments': '*_segments.table'
    }
    
    results = {}
    
    print(f"🔍 Searching in directory: {search_directory}")
    print(f"📁 Directory exists: {os.path.exists(search_directory)}")
    print("=" * 60)
    
    for file_type, pattern in patterns.items():
        print(f"\n🔎 Searching for {file_type} files...")
        print(f"📋 Pattern: {pattern}")
        
        # globを使って再帰的に検索
        search_pattern = os.path.join(search_directory, "**", pattern)
        found_files = glob.glob(search_pattern, recursive=True)
        
        # 結果を格納
        results[file_type] = sorted(found_files)
        
        print(f"✅ Found {len(found_files)} files:")
        for i, file_path in enumerate(found_files, 1):
            # ファイル名からサンプル名を抽出
            filename = os.path.basename(file_path)
            sample_name = filename.replace(f"_{file_type}.table", "").replace("_getpileupsummaries.table", "")
            print(f"  [{i:2d}] {sample_name}: {file_path}")
    
    # 統計情報を表示
    print("\n" + "=" * 60)
    print("📊 Summary:")
    for file_type, files in results.items():
        print(f"  {file_type}: {len(files)} files")
    
    # サンプル名の整合性チェック
    print("\n🔍 Sample consistency check:")
    all_samples = set()
    for file_type, files in results.items():
        samples = set()
        for file_path in files:
            filename = os.path.basename(file_path)
            if file_type == 'getpileupsummaries':
                sample = filename.replace("_getpileupsummaries.table", "")
            elif file_type == 'calculatecontamination':
                sample = filename.replace("_calculatecontamination.table", "")
            elif file_type == 'segments':
                sample = filename.replace("_segments.table", "")
            samples.add(sample)
        print(f"  {file_type}: {len(samples)} unique samples")
        all_samples.update(samples)
    
    print(f"  Total unique samples: {len(all_samples)}")
    
    # ファイルが見つからないサンプルをチェック
    missing_files = {}
    for sample in all_samples:
        missing_files[sample] = []
        for file_type in patterns.keys():
            found = False
            for file_path in results[file_type]:
                if sample in os.path.basename(file_path):
                    found = True
                    break
            if not found:
                missing_files[sample].append(file_type)
    
    incomplete_samples = {k: v for k, v in missing_files.items() if v}
    if incomplete_samples:
        print(f"\n⚠️  Samples with missing files: {len(incomplete_samples)}")
        for sample, missing in incomplete_samples.items():
            print(f"  {sample}: missing {', '.join(missing)}")
    else:
        print("\n✅ All samples have complete file sets!")
    
    # 結果をファイルに保存
    if output_file:
        save_results(results, output_file, search_directory)
    
    return results

def save_results(results, output_file, search_directory):
    """結果をパスのみのリストとして保存"""
    
    print(f"\n💾 Saving path-only lists:")
    
    base_name = output_file.replace('.txt', '').replace('.json', '')
    
    # ファイル種類別にパスのみのリストを保存
    for file_type, files in results.items():
        paths_only_file = f"{base_name}_{file_type}_paths.txt"
        with open(paths_only_file, 'w') as f:
            for file_path in files:
                f.write(f"{file_path}\n")
        
        print(f"  📝 {file_type}: {paths_only_file} ({len(files)} files)")
    
    # 全ファイルのパスを一つのファイルにまとめて保存（オプション）
    all_paths_file = f"{base_name}_all_paths.txt"
    with open(all_paths_file, 'w') as f:
        f.write("# All contamination files\n")
        for file_type, files in results.items():
            f.write(f"# {file_type.upper()} FILES\n")
            for file_path in files:
                f.write(f"{file_path}\n")
            f.write("\n")
    
    print(f"  📋 All files: {all_paths_file}")

def create_python_dict_file(results, output_file, search_directory):
    """この関数は使用しないので削除"""
    pass

def create_file_symlinks(results, target_directory):
    """
    見つかったファイルを指定ディレクトリにシンボリックリンクとして作成
    (オプション機能)
    """
    os.makedirs(target_directory, exist_ok=True)
    
    print(f"\n🔗 Creating symlinks in: {target_directory}")
    
    for file_type, files in results.items():
        for file_path in files:
            filename = os.path.basename(file_path)
            symlink_path = os.path.join(target_directory, filename)
            
            if os.path.exists(symlink_path):
                os.remove(symlink_path)
            
            os.symlink(os.path.abspath(file_path), symlink_path)
            print(f"  ✅ {filename}")

# メイン実行部分
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Search for contamination analysis files")
    parser.add_argument("directory", help="Directory to search in")
    parser.add_argument("-o", "--output", help="Output file path (without extension)")
    parser.add_argument("--symlink-dir", help="Create symlinks in specified directory")
    
    args = parser.parse_args()
    
    # ファイル検索実行
    results = find_contamination_files(args.directory, args.output)
    
    # シンボリックリンク作成（オプション）
    if args.symlink_dir:
        create_file_symlinks(results, args.symlink_dir)

# 使用例（スクリプト内で直接実行する場合）
# search_directory = "/pi/michael.lodato-umw/home/waka.ujita-umw/miniconda3/CellLineage/contamination_results"
# results = find_contamination_files(search_directory, "contamination_files_list")
