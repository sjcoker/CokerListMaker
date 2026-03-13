# Coker's List Maker (v10.5.2)
# Advanced ETL Engine with Dual-Core Processing (RAM/SQLite) and Analytics
# Original concept and name by Steven James Coker (November 1993).
# Developed in collaboration with Google's AI, 2026.

import tkinter as tk
from tkinter import filedialog, scrolledtext, messagebox, ttk
import os
import tempfile
import csv
import stat
from datetime import datetime
import threading
import hashlib
import time
import io
import queue
import sqlite3
import ctypes
import heapq
import multiprocessing
import concurrent.futures
from collections import Counter

# Windows File Attributes
FILE_ATTRIBUTE_REPARSE_POINT = 0x400
FILE_ATTRIBUTE_OFFLINE = 0x1000
FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS = 0x400000 

# Windows Memory Check Structure
class MEMORYSTATUSEX(ctypes.Structure):
    _fields_ = [
        ("dwLength", ctypes.c_ulong),
        ("dwMemoryLoad", ctypes.c_ulong),
        ("ullTotalPhys", ctypes.c_ulonglong),
        ("ullAvailPhys", ctypes.c_ulonglong),
        ("ullTotalPageFile", ctypes.c_ulonglong),
        ("ullAvailPageFile", ctypes.c_ulonglong),
        ("ullTotalVirtual", ctypes.c_ulonglong),
        ("ullAvailVirtual", ctypes.c_ulonglong),
        ("sullAvailExtendedVirtual", ctypes.c_ulonglong),
    ]

def get_free_ram_mb():
    stat_val = MEMORYSTATUSEX()
    stat_val.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
    ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat_val))
    return stat_val.ullAvailPhys / (1024 * 1024)

def parallel_partial_hash(task):
    filepath, file_size, chunk_mb_val = task
    sha256 = hashlib.sha256()
    try:
        chunk_size = int(float(chunk_mb_val) * 1048576)
    except ValueError:
        chunk_size = 1048576
    if chunk_size <= 0: chunk_size = 1048576
    
    try:
        with open(filepath, 'rb') as f:
            if file_size <= (chunk_size * 2):
                sha256.update(f.read())
            else:
                sha256.update(f.read(chunk_size))
                f.seek(file_size - chunk_size)
                sha256.update(f.read(chunk_size))
        return filepath, file_size, sha256.hexdigest()
    except Exception:
        return filepath, file_size, None

def parallel_full_hash(task):
    filepath, item_dict = task
    sha256 = hashlib.sha256()
    try:
        with open(filepath, 'rb') as f:
            while chunk := f.read(8388608): 
                sha256.update(chunk)
        return filepath, item_dict, sha256.hexdigest()
    except Exception: 
        return filepath, item_dict, None

class ListMakerApp(tk.Tk):
    def __init__(self):
        super().__init__()

        self.stop_event = threading.Event()
        self.is_running = False
        self.version = "v10.5.2"
        self.title(f"Coker's List Maker {self.version}")
        try:
            icon_path = os.path.join(os.path.dirname(__file__), "assets", "icon.ico")
            if os.path.exists(icon_path):
                self.iconbitmap(icon_path)
            myappid = f'stevenjamescoker.listmaker.{self.version}' 
            ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(myappid)
        except Exception:
            pass 

        self.geometry("1024x900")
        self.internal_flush_limit = 50 * 1024 * 1024 
        self.ui_queue = queue.Queue()
        self.engine_status_msg = "Ready"

        self.protocol("WM_DELETE_WINDOW", self.hard_exit)

        intro_text = (
            f"Welcome to Coker's List Maker {self.version}\n"
            "An advanced, multi-core data engine that maximizes CPU usage while safeguarding system RAM.\n"
            "Build Queue ➔ Select Mode ➔ Configure Options ➔ Generate List"
        )
        tk.Label(self, text=intro_text, justify=tk.CENTER, font=('TkDefaultFont', 10, 'bold')).pack(pady=(5, 10))

        top_frame = tk.Frame(self)
        top_frame.pack(side='top', fill='x', padx=10, pady=2)
        mid_frame = tk.Frame(self)
        mid_frame.pack(side='top', fill='x', padx=10, pady=2)
        mid_frame.grid_columnconfigure(0, weight=1)
        mid_frame.grid_columnconfigure(1, weight=1)
        bot_frame = tk.Frame(self)
        bot_frame.pack(side='top', fill='x', padx=10, pady=2)

        # --- 1. DIRECTORY QUEUE ---
        self.queue_frame = tk.LabelFrame(top_frame, text="1. Target Directories (Queue)", padx=10, pady=5)
        self.queue_frame.pack(fill='x')
        btn_frame = tk.Frame(self.queue_frame)
        btn_frame.pack(side='right', fill='y')
        tk.Button(btn_frame, text="Add Folder/Drive", command=self.add_directory, width=15).pack(pady=2, padx=(5,0))
        tk.Button(btn_frame, text="Clear Queue", command=lambda: self.dir_listbox.delete(0, tk.END), width=15).pack(pady=2, padx=(5,0))
        listbox_frame = tk.Frame(self.queue_frame)
        listbox_frame.pack(side='left', fill='both', expand=True)
        scrollbar = ttk.Scrollbar(listbox_frame, orient="vertical")
        self.dir_listbox = tk.Listbox(listbox_frame, height=4, yscrollcommand=scrollbar.set)
        scrollbar.config(command=self.dir_listbox.yview)
        scrollbar.pack(side='right', fill='y')
        self.dir_listbox.pack(side='left', fill='both', expand=True)

        left_column = tk.Frame(mid_frame)
        left_column.grid(row=0, column=0, sticky='new', padx=(0, 5))
        right_column = tk.Frame(mid_frame)
        right_column.grid(row=0, column=1, sticky='new', padx=(5, 0))

        # --- 2. OPERATING MODE ---
        self.mode_frame = tk.LabelFrame(left_column, text="2. Operating Mode", padx=10, pady=5)
        self.mode_frame.pack(fill='x', pady=2)
        self.op_mode = tk.StringVar(value='standard')
        tk.Radiobutton(self.mode_frame, text="Standard Directory List", variable=self.op_mode, value='standard', command=self._update_ui_state).pack(anchor='w')
        tk.Radiobutton(self.mode_frame, text="Smart Duplicate Hunter (Hash beginning and end of files)", variable=self.op_mode, value='redundancy', command=self._update_ui_state).pack(anchor='w')
        self.run_tier4 = tk.BooleanVar(value=False)
        self.tier4_chk = tk.Checkbutton(self.mode_frame, text="Full Hash Verification (Bit-Perfect Match Confirmation)", variable=self.run_tier4)
        self.tier4_chk.pack(anchor='w', padx=20) 
        self.tier4_lbl = tk.Label(self.mode_frame, text="Smart Hunter finds likely duplicates based on size and partial hash. \nFull Hash confirms exact matches, but takes much longer.", font=('', 9, 'italic'), wraplength=450, justify='left')
        self.tier4_lbl.pack(anchor='w', padx=2, pady=(2,10))

        # --- 3. STANDARD LIST OPTIONS ---
        self.list_frame = tk.LabelFrame(right_column, text="3. Standard List Options", padx=10, pady=5)
        self.list_frame.pack(fill='x', pady=2)
        self.list_choice = tk.StringVar(value='a')
        tk.Radiobutton(self.list_frame, text="Files Only", variable=self.list_choice, value='f').grid(row=0, column=0, sticky='w')
        tk.Radiobutton(self.list_frame, text="Directories Only", variable=self.list_choice, value='d').grid(row=0, column=1, sticky='w', padx=(10,0))
        tk.Radiobutton(self.list_frame, text="All Directories & Files", variable=self.list_choice, value='a').grid(row=1, column=0, sticky='w')
        tk.Radiobutton(self.list_frame, text="Top Dir Contents Only", variable=self.list_choice, value='t').grid(row=1, column=1, sticky='w', padx=(10,0))
        
        # NEW SURGICAL INJECTION: Max Folder Depth UI
        depth_frame = tk.Frame(self.list_frame)
        depth_frame.grid(row=2, column=0, columnspan=2, sticky='w', pady=(5,0))
        tk.Label(depth_frame, text="Max Subfolder Depth:").pack(side='left')
        self.max_depth_var = tk.StringVar(value="0")
        tk.Spinbox(depth_frame, from_=0, to=99, textvariable=self.max_depth_var, width=5, justify='center').pack(side='left', padx=5)
        tk.Label(depth_frame, text="(0 = Unlimited/Fully Recursive)").pack(side='left')

        # --- 4. FILTERS ---
        self.filter_frame = tk.LabelFrame(left_column, text="4. Filters", padx=10, pady=5)
        self.filter_frame.pack(fill='x', pady=2)
        tk.Label(self.filter_frame, text="Skip files < (KB):").grid(row=0, column=0, sticky='w', pady=2)
        self.min_size_kb = tk.StringVar(value="0")
        self.min_size_kb_entry = tk.Entry(self.filter_frame, textvariable=self.min_size_kb, width=8, justify='center')
        self.min_size_kb_entry.grid(row=0, column=1, sticky='w', pady=2, padx=5)
        tk.Label(self.filter_frame, text="(Skip small files in Dup Hunter)").grid(row=0, column=2, columnspan=2, sticky='w', pady=2)
        tk.Label(self.filter_frame, text="Skip files > (MB):").grid(row=1, column=0, sticky='w', pady=2)
        self.max_size_mb = tk.StringVar(value="0") 
        self.max_size_mb_entry = tk.Entry(self.filter_frame, textvariable=self.max_size_mb, width=8, justify='center')
        self.max_size_mb_entry.grid(row=1, column=1, sticky='w', pady=2, padx=5)
        tk.Label(self.filter_frame, text="(Skip large files or 0 = Off)").grid(row=1, column=2, columnspan=2, sticky='w', pady=2)
        self.skip_hidden = tk.BooleanVar(value=True)
        tk.Checkbutton(self.filter_frame, text="Skip Hidden/System Files", variable=self.skip_hidden).grid(row=2, column=0, columnspan=2, sticky='w')
        self.skip_sensitive = tk.BooleanVar(value=True)
        tk.Checkbutton(self.filter_frame, text="Skip OS Folders & Offline/Cloud Files", variable=self.skip_sensitive).grid(row=2, column=2, columnspan=2, sticky='w')
        tk.Label(self.filter_frame, text="Skip OS Folders and offline files to reduce chance of false Antivirus alerts.", font=('', 9, 'italic'), wraplength=450, justify='left').grid(row=3, column=0, columnspan=4, sticky='w', pady=(6,10))

        # --- 5. PROCESSING OPTIONS ---
        self.proc_frame = tk.LabelFrame(right_column, text="5. Processing Options", padx=10, pady=5)
        self.proc_frame.pack(fill='x', pady=2)
        self.engine_choice = tk.StringVar(value='ram')
        tk.Radiobutton(self.proc_frame, text="RAM Workspace (Fastest, requires ~1GB RAM per 1M files)", variable=self.engine_choice, value='ram', command=self._update_ui_state, justify='left').grid(row=0, column=0, columnspan=3, sticky='w', pady=(0,2))
        tk.Radiobutton(self.proc_frame, text="Drive Workspace (For massive queues in limited memory.", variable=self.engine_choice, value='sql', command=self._update_ui_state, justify='left').grid(row=1, column=0, columnspan=3, sticky='w', pady=(0,2))
        self.sqlite_dir = tk.StringVar(value=tempfile.gettempdir())
        self.sql_dir_label = tk.Label(self.proc_frame, text="Drive Workspace:")
        self.sql_dir_label.grid(row=2, column=0, sticky='w', pady=2)
        self.sql_dir_frame = tk.Frame(self.proc_frame)
        self.sql_dir_frame.grid(row=2, column=1, columnspan=2, sticky='w', pady=2)
        self.sql_dir_entry = tk.Entry(self.sql_dir_frame, textvariable=self.sqlite_dir, width=40, state='disabled')
        self.sql_dir_entry.pack(side='left', padx=5)
        self.sql_dir_btn = tk.Button(self.sql_dir_frame, text="Change", command=self._change_sqlite_dir, state='disabled')
        self.sql_dir_btn.pack(side='left', padx=(3, 0))
        self.chunk_mb = tk.StringVar(value="0")
        tk.Label(self.proc_frame, text="TXT Chunks (MB, 0=Off):").grid(row=3, column=0, sticky='w', pady=2)
        chunk_frame = tk.Frame(self.proc_frame)
        chunk_frame.grid(row=3, column=1, columnspan=2, sticky='w', pady=2)
        tk.Entry(chunk_frame, textvariable=self.chunk_mb, width=8, justify='center').pack(side='left', padx=(5, 10))
        tk.Label(chunk_frame, text="(~3 MB for AI Context Windows)").pack(side='left')
        self.hash_chunk_mb = tk.StringVar(value="1")
        tk.Label(self.proc_frame, text="Partial Hash (MB):").grid(row=4, column=0, sticky='w', pady=2)
        hash_frame = tk.Frame(self.proc_frame)
        hash_frame.grid(row=4, column=1, columnspan=2, sticky='w', pady=2)
        self.hash_entry = tk.Entry(hash_frame, textvariable=self.hash_chunk_mb, width=8, justify='center')
        self.hash_entry.pack(side='left', padx=(5, 10))
        tk.Label(hash_frame, text="(1 MB Recommended)").pack(side='left')
        sys_cores = os.cpu_count() or 4
        tk.Label(self.proc_frame, text="Cores to Leave Free:").grid(row=5, column=0, sticky='w', pady=2)
        cores_frame = tk.Frame(self.proc_frame)
        cores_frame.grid(row=5, column=1, columnspan=2, sticky='w', pady=2)
        self.reserved_cores = tk.StringVar(value="2")
        max_reserve = max(1, sys_cores - 1)
        self.cores_spinbox = tk.Spinbox(cores_frame, from_=1, to=max_reserve, textvariable=self.reserved_cores, width=6, justify='center')
        self.cores_spinbox.pack(side='left', padx=(5, 10))
        tk.Label(cores_frame, text=f"(Total Processor Cores: {sys_cores})").pack(side='left')
        
        # --- 6. OUTPUT OPTIONS ---
        self.out_frame = tk.LabelFrame(bot_frame, text="6. Output Options", padx=10, pady=5)
        self.out_frame.pack(fill='x', pady=2)
        self.out_frame.grid_columnconfigure(0, weight=1)
        self.out_frame.grid_columnconfigure(1, weight=1)
        self.out_frame.grid_columnconfigure(2, weight=1)

        out_col1 = tk.Frame(self.out_frame)
        out_col1.grid(row=0, column=0, sticky='nw')
        tk.Label(out_col1, text="Destination:", font=('', 9, 'bold')).pack(anchor='w')
        self.output_dest = tk.StringVar(value='full')
        tk.Radiobutton(out_col1, text="Full Reports (Summary + Detailed Lists)", variable=self.output_dest, value='full', command=self._update_ui_state).pack(anchor='w')
        tk.Radiobutton(out_col1, text="Summary to Screen Only (Good for Low RAM)", variable=self.output_dest, value='screen', command=self._update_ui_state).pack(anchor='w')
        tk.Radiobutton(out_col1, text="Summary to Screen and TXT (Good for Low RAM)", variable=self.output_dest, value='summary', command=self._update_ui_state).pack(anchor='w')
        tk.Frame(out_col1, height=2, bd=1, relief=tk.SUNKEN).pack(fill='x', pady=4)
        self.queue_mode = tk.StringVar(value='lumped')
        tk.Radiobutton(out_col1, text="Lumped Report for all Queued Targets", variable=self.queue_mode, value='lumped').pack(anchor='w')
        tk.Radiobutton(out_col1, text="Separate Reports for each Queued Target", variable=self.queue_mode, value='separate').pack(anchor='w')

        self.out_col2 = tk.Frame(self.out_frame)
        self.out_col2.grid(row=0, column=1, sticky='nw')
        tk.Label(self.out_col2, text="Included Data Columns:", font=('', 9, 'bold')).pack(anchor='w')
        self.include_path = tk.BooleanVar(value=True)
        self.include_name = tk.BooleanVar(value=True)
        self.include_cdate = tk.BooleanVar(value=False)
        self.include_mdate = tk.BooleanVar(value=False)
        self.include_time = tk.BooleanVar(value=False)
        self.include_size = tk.BooleanVar(value=False)
        tk.Checkbutton(self.out_col2, text="Folder Path", variable=self.include_path, command=self._check_data_state).pack(anchor='w')
        tk.Checkbutton(self.out_col2, text="File Name", variable=self.include_name, command=self._check_data_state).pack(anchor='w')
        tk.Checkbutton(self.out_col2, text="File Size", variable=self.include_size).pack(anchor='w')
        tk.Checkbutton(self.out_col2, text="Created Date", variable=self.include_cdate).pack(anchor='w')
        tk.Checkbutton(self.out_col2, text="Modified Date", variable=self.include_mdate).pack(anchor='w')
        tk.Checkbutton(self.out_col2, text="Modified Time", variable=self.include_time).pack(anchor='w')

        out_col3 = tk.Frame(self.out_frame)
        out_col3.grid(row=0, column=2, sticky='nw')
        tk.Label(out_col3, text="File Formats (For Full Reports):", font=('', 9, 'bold')).pack(anchor='w')
        self.output_txt = tk.BooleanVar(value=True)
        self.output_csv = tk.BooleanVar(value=False)
        self.txt_chk = tk.Checkbutton(out_col3, text="TXT Report", variable=self.output_txt)
        self.txt_chk.pack(anchor='w')
        self.csv_chk = tk.Checkbutton(out_col3, text="CSV Data", variable=self.output_csv)
        self.csv_chk.pack(anchor='w')
        tk.Label(out_col3, text="Full Reports using RAM Workspace safely abort if memory drops below 1 GB. Use Drive Workspace for massive queues. Text files exceeding 250 MB are split into parts for safe viewing.", font=('', 9, 'italic'), wraplength=320, justify='left').pack(anchor='w', pady=(5,0))
        # text="Full Reports using RAM Workspace will safely abort if memory drops below 1 GB. For massive queues, use Drive Workspace.\n\nText reports over 250 MB are split into parts to prevent text editor crashes."
        # text="• RAM Engine safely aborts if memory is < 1 GB.\n• Use Drive Workspace for massive queues.\n• Text reports over 250 MB are automatically split into parts to ensure safe opening.", 


# --- ACTION BUTTONS (Updated to include About) ---
        action_frame = tk.Frame(self)
        action_frame.pack(pady=5, padx=15, fill='x') 

        # Left side: The Buttons
        tk.Button(action_frame, text="About", command=self.show_about, font=('TkDefaultFont', 10, 'bold'), width=12).pack(side='left', padx=10)
        self.run_button = tk.Button(action_frame, text="Generate", command=self.pre_flight_check, font=('TkDefaultFont', 10, 'bold'), width=12)
        self.run_button.pack(side='left', padx=10)

        self.stop_button = tk.Button(action_frame, text="Stop", command=self.stop_processing, state='disabled', font=('TkDefaultFont', 10, 'bold'), width=12, fg='red')
        self.stop_button.pack(side='left', padx=10)

        # NEW: The About Button
        tk.Button(action_frame, text="Exit", command=self.hard_exit, font=('TkDefaultFont', 10, 'bold'), width=12).pack(side='left', padx=10)
        
        # Right side: Dark Mode Toggle
        self.dark_mode = tk.BooleanVar(value=False)
        self.dark_chk = tk.Checkbutton(action_frame, text="Dark Mode Console", variable=self.dark_mode, command=self._toggle_console_colors)
        self.dark_chk.pack(side='right', padx=20)

        self.status_var = tk.StringVar(value="Ready")
        self.status_label = tk.Label(self, textvariable=self.status_var, relief=tk.SUNKEN, anchor='w', padx=5, font=('TkDefaultFont', 9, 'bold'))
        self.status_label.pack(side='bottom', fill='x')

        self.output_text = scrolledtext.ScrolledText(self, wrap=tk.NONE, state='disabled', font=("TkFixedFont", 9), bg='white', fg='black')
        self.output_text.pack(expand=True, fill='both', padx=10, pady=(0, 5))
        
        self.stats = self._reset_stats()
        self.save_directory = ""
        self._update_ui_state()
        self._poll_ui()

    def _get_file_header(self):
        return f"""# ---------------------------------------------------------------------
# Coker's List Maker {self.version}.
#
# Copyright by Steven James Coker. All rights reserved. 
# Original command-line script by Steven James Coker (November 1993).
# Modern GUI developed in collaboration with Google's AI (2026).
# Developer Portfolio: https://github.com/sjcoker
#
# LICENSE (DONATIONWARE):
# This program is free for non-commercial use. You may copy and 
# distribute it freely. You may contact the author SJCoker1 
# using either gmail or yahoo.
#
# SUPPORT THE WORK:
# If you find this program helpful, please consider a donation.
#  - PayPal:   https://www.paypal.com/paypalme/SJCoker
#
# ---------------------------------------------------------------------\n"""

    def show_about(self):
        # Create a custom popup window
        about_win = tk.Toplevel(self)
        about_win.title("About Coker's List Maker")
        about_win.geometry("550x380")
        about_win.resizable(False, False)
        
        # Force it to use your custom icon
        try: about_win.iconbitmap(os.path.join(os.path.dirname(__file__), "assets", "icon.ico"))
        except: pass

        # Create a text box that users can copy from, but not type in
        txt = scrolledtext.ScrolledText(about_win, wrap=tk.WORD, font=("TkFixedFont", 9), bg='#f0f0f0', relief=tk.FLAT)
        txt.pack(expand=True, fill='both', padx=20, pady=20)
        
        # Insert your clean header text
        clean_text = self._get_file_header().replace('# ', '').replace('#', '')
        txt.insert(1.0, clean_text)
        txt.config(state='disabled') # Locks the text so they can't edit it

        # Add a simple close button
        tk.Button(about_win, text="Close", command=about_win.destroy, width=10).pack(pady=(0, 20))

        # Keep the popup in front of the main window
        about_win.transient(self)
        about_win.grab_set()

    def _toggle_console_colors(self):
        if self.dark_mode.get():
            self.output_text.config(bg='black', fg='lightgreen')
        else:
            self.output_text.config(bg='white', fg='black')

    def _poll_ui(self):
        if self.is_running and self.stats['start_time'] > 0:
            elapsed = int(time.time() - self.stats['start_time'])
            mins, secs = divmod(elapsed, 60)
            hrs, mins = divmod(mins, 60)
            time_str = f"{hrs:02d}:{mins:02d}:{secs:02d}"
            hash_str = ""
            workers = self.stats.get('workers', 0)
            worker_str = f" [{workers} Cores]" if workers > 0 else ""
            if self.stats.get('total_to_full_hash', 0) > 0:
                hash_str = f" | Full Hashed{worker_str}: {self.stats.get('full_hashed', 0):,} / {self.stats['total_to_full_hash']:,}"
            elif self.stats.get('total_to_hash', 0) > 0:
                hash_str = f" | Partial Hashed{worker_str}: {self.stats['hashed']:,} / {self.stats['total_to_hash']:,}"
            current = self.stats.get('current_target', '')
            drive_str = f" [{os.path.splitdrive(current)[0]}]" if current else ""
            self.title(f"Coker's List Maker {self.version} - Scanning{drive_str}")
            full_status = f"{self.engine_status_msg}{drive_str} | Processed: {self.stats['scanned']:,}{hash_str} | Elapsed Time: {time_str}"
            self.status_var.set(full_status)
        else:
            self.title(f"Coker's List Maker {self.version}")
            self.status_var.set(self.engine_status_msg)
        while not self.ui_queue.empty():
            msg = self.ui_queue.get()
            self.output_text.config(state='normal')
            self.output_text.insert(tk.END, msg + "\n")
            self.output_text.see(tk.END)
            self.output_text.config(state='disabled')
        self.after(100, self._poll_ui)

    def _reset_stats(self):
        return {
            'current_target': '', 'scanned': 0, 'hashed': 0, 'total_to_hash': 0, 
            'full_hashed': 0, 'total_to_full_hash': 0, 'workers': 0, 
            'dupes_found': 0, 'reclaimable_bytes': 0,
            'files_generated': 0, 'bytes_generated': 0, 'generated_paths': [],
            'start_time': 0, 'end_time': 0, 'targets': {},
            'scan_time_sec': 0, 'write_time_sec': 0, 'true_dupes': {} 
        }

    def _init_target_stats(self, target):
        if target not in self.stats['targets']:
            self.stats['targets'][target] = {
                'total_files': 0, 'total_size': 0, 
                'skipped_hidden': 0, 'skipped_small': 0, 'skipped_large': 0,
                'skipped_offline': 0, # <-- ADDED THIS
                'oldest': None, 'newest': None,
                'top_10': [], 'extensions': Counter(), 'true_dupes': {}
            }

    def _update_ui_state(self):
        mode = self.op_mode.get()
        dest = self.output_dest.get()
        engine = self.engine_choice.get() 
        if mode == 'redundancy':
            if self.min_size_kb.get() == "0": self.min_size_kb.set("2")
            self.min_size_kb_entry.focus_set()
            self.tier4_chk.config(state='normal')
            self.hash_entry.config(state='normal')
            self.cores_spinbox.config(state='normal')
            self.tier4_lbl.config(state='normal')
        elif mode == 'standard':
            if self.min_size_kb.get() == "2": self.min_size_kb.set("0")
            self.tier4_chk.config(state='disabled')
            self.run_tier4.set(False)
            self.hash_entry.config(state='disabled')
            self.cores_spinbox.config(state='disabled')
            self.tier4_lbl.config(state='disabled')
        if engine == 'sql':
            self.sql_dir_label.config(state='normal')
            self.sql_dir_entry.config(state='normal')
            self.sql_dir_btn.config(state='normal')
        else:
            self.sql_dir_label.config(state='disabled')
            self.sql_dir_entry.config(state='disabled')
            self.sql_dir_btn.config(state='disabled')
        list_state = 'normal' if mode == 'standard' else 'disabled'
        self._set_widget_state(self.list_frame, list_state)
        if dest in ('screen', 'summary'):
            data_state = 'disabled'
            file_state = 'disabled' if dest == 'screen' else 'normal'
            self.txt_chk.config(state=file_state)
            self.csv_chk.config(state='disabled')
            self.include_path.set(False)
            self.include_name.set(False)
            self.include_cdate.set(False)
            self.include_mdate.set(False)
            self.include_time.set(False)
            self.include_size.set(False)
            self.output_csv.set(False) 
            if dest == 'screen': self.output_txt.set(False) 
            elif dest == 'summary': self.output_txt.set(True)  
        else:
            data_state = 'normal' if mode == 'standard' else 'disabled'
            self.txt_chk.config(state='normal')
            self.csv_chk.config(state='normal')
            if not self.include_path.get() and not self.include_name.get():
                self.include_path.set(True)
                self.include_name.set(True)
            if not self.output_txt.get() and not self.output_csv.get():
                self.output_txt.set(True)
        for child in self.out_col2.winfo_children()[1:]:
            try: child.config(state=data_state)
            except tk.TclError: pass

    def _check_data_state(self):
        if not self.include_path.get() and not self.include_name.get():
            self.output_dest.set('screen')
            self._update_ui_state()

    def _set_widget_state(self, widget, state):
        try: widget.config(state=state)
        except tk.TclError: pass
        for child in widget.winfo_children(): self._set_widget_state(child, state)

    def _change_sqlite_dir(self):
        path = filedialog.askdirectory(title="Select Temporary SQLite Working Directory")
        if path: self.sqlite_dir.set(os.path.normpath(path))

    def add_directory(self):
        path = filedialog.askdirectory()
        if path: self.dir_listbox.insert(tk.END, os.path.normpath(path))

    def pre_flight_check(self):
        queue_targets = list(self.dir_listbox.get(0, tk.END))
        if not queue_targets:
            messagebox.showerror("Error", "Please add at least one directory to the queue.", parent=self)
            return
        if self.output_dest.get() in ('full', 'summary'):
            if self.output_dest.get() == 'full' and not (self.output_txt.get() or self.output_csv.get()):
                messagebox.showerror("Error", "Please select an output file format.", parent=self)
                return
            self.save_directory = filedialog.askdirectory(title="Select Destination Folder for Reports")
            if not self.save_directory: return 
            
            # --- NEW: Chunking Size Validation ---
        try:
            chunk_val = float(self.chunk_mb.get() or 0)
            if chunk_val > 250:
                warn_msg = (f"You have set a TXT Chunk size of {chunk_val} MB.\n\n"
                            "Files over 250 MB can be very slow or cause crashes in standard "
                            "editors like Windows Notepad.\n\n"
                            "Do you want to proceed with this large chunk size?")
                if not messagebox.askyesno("Large File Warning", warn_msg, parent=self):
                    return # Stop and let them change it
        except ValueError:
            pass # The validator handles non-numeric input elsewhere

        target_names = ", ".join(queue_targets)
        if len(target_names) > 65: target_names = target_names[:62] + "..."
        summary = f"TARGET QUEUE: {len(queue_targets)} location(s) [{target_names}]\nMODE: {'Smart Duplicate Hunter' if self.op_mode.get() == 'redundancy' else 'Standard Directory List'}\n"
        if self.output_dest.get() in ('full', 'summary') and self.save_directory:
            summary += f"SAVE TO: {self.save_directory}\n"
        if self.output_dest.get() == 'full' and self.engine_choice.get() == 'ram':
            summary += "\nSYSTEM NOTICE:\n'Full Reports' with 'In-Memory Engine' selected. Requires approx. 1 GB RAM per 1 million files.\n"
        summary += "\nDo you wish to proceed?"
        if messagebox.askyesno("Pre-Flight Confirmation", summary, parent=self): self.start_processing()

    def start_processing(self):
        self.stop_event.clear()
        self.stats = self._reset_stats()
        self.stats['start_time'] = time.time()
        self.is_running = True
        for frame in [self.queue_frame, self.mode_frame, self.list_frame, self.filter_frame, self.proc_frame, self.out_frame]:
            self._set_widget_state(frame, 'disabled')
        self.run_button.config(state='disabled')
        self.stop_button.config(state='normal')
        
        self.output_text.config(state='normal')
        self.output_text.delete(1.0, tk.END)
        self.output_text.config(state='disabled')
        
        # --- NEW: Print Header to Screen immediately ---
        self.ui_queue.put(self._get_file_header().strip()) 
        self.ui_queue.put("\n[SYSTEM] Processing initiated...")
        
        threading.Thread(target=self.process_router, daemon=True).start()

    def stop_processing(self):
        self.stop_event.set()
        self.is_running = False
        self.engine_status_msg = "Halting engine... Please wait."

# NEW SURGICAL INJECTION: Depth tracking added to recursive scan
    def _fast_scandir(self, directory, recursive=True, skip_sensitive=False, current_depth=1, max_depth=0):
        # ... [setup code] ...
        restricted_folders = {'appdata', 'windows', 'program files', 'program files (x86)', 'programdata', '$recycle.bin', 'system volume information'}
        try:
            with os.scandir(directory) as it:
                for entry in it:
                    if self.stop_event.is_set(): break
                    is_dir = entry.is_dir(follow_symlinks=False)
                    if skip_sensitive and is_dir:
                        if entry.name.lower() in restricted_folders: continue
                    yield entry
                    if recursive and is_dir and not entry.is_symlink():
                        # If max_depth is 0, recurse infinitely. Else, only recurse if under the limit.
                        if max_depth == 0 or current_depth < max_depth:
                            yield from self._fast_scandir(entry.path, recursive=True, skip_sensitive=skip_sensitive, current_depth=current_depth+1, max_depth=max_depth)
        except (PermissionError, OSError): pass

    def _update_analytics(self, target, entry, is_dir, stat_info):
        self.stats['targets'][target]['total_files'] += 1
        if not is_dir:
            sz = stat_info.st_size
            self.stats['targets'][target]['total_size'] += sz
            c_date_val = stat_info.st_ctime
            if hasattr(stat_info, 'st_birthtime') and stat_info.st_birthtime:
                c_date_val = stat_info.st_birthtime
            if self.stats['targets'][target]['oldest'] is None or c_date_val < self.stats['targets'][target]['oldest'][0]:
                self.stats['targets'][target]['oldest'] = (c_date_val, entry.path)
            if self.stats['targets'][target]['newest'] is None or c_date_val > self.stats['targets'][target]['newest'][0]:
                self.stats['targets'][target]['newest'] = (c_date_val, entry.path)
            ext = os.path.splitext(entry.name)[1].lower()
            if ext: self.stats['targets'][target]['extensions'][ext] += 1
            heap = self.stats['targets'][target]['top_10']
            if len(heap) < 10: heapq.heappush(heap, (sz, entry.path))
            elif sz > heap[0][0]: heapq.heapreplace(heap, (sz, entry.path))

    def process_router(self):
        try: min_sz = float(self.min_size_kb.get() or 0) * 1024 
        except ValueError: min_sz = 0
        try: max_sz = float(self.max_size_mb.get() or 0) * 1024 * 1024 
        except ValueError: max_sz = 0
        self._current_skip_sensitive = self.skip_sensitive.get()
        self._current_skip_hidden = self.skip_hidden.get()
        queue_targets = list(self.dir_listbox.get(0, tk.END))
        is_lumped = (self.queue_mode.get() == 'lumped')
        if is_lumped:
            for q in queue_targets: self._init_target_stats(q)
            self._execute_scan(queue_targets, min_sz, max_sz)
        else:
            for base_dir in queue_targets:
                if self.stop_event.is_set(): break
                self._init_target_stats(base_dir)
                self._execute_scan([base_dir], min_sz, max_sz)
        self.stats['end_time'] = time.time()
        self.display_analytics_dashboard()
        self.is_running = False
        self.after(0, self._reset_ui_after_run)

    def _reset_ui_after_run(self):
        for frame in [self.queue_frame, self.mode_frame, self.list_frame, self.filter_frame, self.proc_frame, self.out_frame]:
            self._set_widget_state(frame, 'normal')
        self.run_button.config(state='normal')
        self.stop_button.config(state='disabled')
        self._update_ui_state()

    def _execute_scan(self, dir_list, min_size, max_size):
        mode = self.op_mode.get()
        dest = self.output_dest.get()
        engine = self.engine_choice.get()
        t0 = time.time()
        if mode == 'standard':
            results = self.logic_standard_scan(dir_list, min_size, max_size, engine, dest)
            t1 = time.time()
            self.stats['scan_time_sec'] += (t1 - t0)
            if not self.stop_event.is_set() and dest == 'full':
                self.engine_status_msg = "Preparing Standard Reports..."
                self.handle_standard_output(results, dir_list, engine)
                t2 = time.time()
                self.stats['write_time_sec'] += (t2 - t1)
        else:
            results = self.logic_redundancy_scan(dir_list, min_size, max_size)
            if self.queue_mode.get() == 'separate': self.stats['targets'][dir_list[0]]['true_dupes'] = results
            for h, items in results.items():
                if h in self.stats['true_dupes']: self.stats['true_dupes'][h].extend(items)
                else: self.stats['true_dupes'][h] = items
            t1 = time.time()
            self.stats['scan_time_sec'] += (t1 - t0)
            if not self.stop_event.is_set() and dest == 'full':
                self.engine_status_msg = "Preparing Duplicate Reports..."
                self.handle_redundancy_output(results, dir_list)
                t2 = time.time()
                self.stats['write_time_sec'] += (t2 - t1)

    def logic_standard_scan(self, queue_targets, min_size, max_size, engine, dest):
        list_choice = self.list_choice.get()
        results_ram = []
        db_conn = None
        if engine == 'sql' and dest == 'full':
            work_dir = self.sqlite_dir.get()
            if not work_dir or not os.path.exists(work_dir): work_dir = os.environ.get('TEMP', self.save_directory)
            db_path = os.path.join(work_dir, f"coker_temp_scan_{int(time.time())}.db")
            db_conn = sqlite3.connect(db_path)
            db_conn.execute("PRAGMA journal_mode=WAL;")
            db_cursor = db_conn.cursor()
            db_cursor.execute("CREATE TABLE files (base TEXT, is_dir INT, path TEXT, name TEXT, cdate TEXT, mdate TEXT, mtime TEXT, sz INT)")
        # NEW SURGICAL INJECTION: Grab user's max depth setting
        try: depth_limit = int(self.max_depth_var.get())
        except ValueError: depth_limit = 0

        for base_dir in queue_targets:
            self.stats['current_target'] = base_dir
            recursive = (list_choice != 't')
            
            # Pass the depth_limit to _fast_scandir
            for entry in self._fast_scandir(base_dir, recursive, self._current_skip_sensitive, current_depth=1, max_depth=depth_limit):
                if self.stop_event.is_set(): break
                
                self.stats['scanned'] += 1
                if self.stats['scanned'] % 100000 == 0:
                    self.ui_queue.put(f"> Scanned {self.stats['scanned']:,} files...")
                    if engine == 'ram' and dest == 'full':
                        if get_free_ram_mb() < 1024:
                            self.ui_queue.put("[!!!] CRITICAL: Low RAM. Halting.")
                            self.stop_event.set()
                            break
                try:
                    attrs = entry.stat(follow_symlinks=False).st_file_attributes
                    if bool(attrs & FILE_ATTRIBUTE_REPARSE_POINT): continue
                    
                    if self._current_skip_sensitive and (bool(attrs & FILE_ATTRIBUTE_OFFLINE) or bool(attrs & FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS)):
                        self.stats['targets'][base_dir]['skipped_offline'] += 1
                        continue

                    if self._current_skip_hidden and bool(attrs & (stat.FILE_ATTRIBUTE_HIDDEN | stat.FILE_ATTRIBUTE_SYSTEM)):
                        self.stats['targets'][base_dir]['skipped_hidden'] += 1
                        continue
                    is_dir = entry.is_dir(follow_symlinks=False)
                    if entry.is_symlink(): continue 
                    if (list_choice == 'd' and not is_dir) or (list_choice == 'f' and is_dir): continue
                    stat_info = entry.stat(follow_symlinks=False)
                    if not is_dir:
                        sz = stat_info.st_size
                        if sz < min_size:
                            self.stats['targets'][base_dir]['skipped_small'] += 1
                            continue
                        if max_size > 0 and sz > max_size:
                            self.stats['targets'][base_dir]['skipped_large'] += 1
                            continue
                    self._update_analytics(base_dir, entry, is_dir, stat_info)
                    if dest == 'full':
                        c_date_val = stat_info.st_birthtime if hasattr(stat_info, 'st_birthtime') else stat_info.st_ctime
                        cd_str = datetime.fromtimestamp(c_date_val).strftime("%Y-%m-%d")
                        md_str = datetime.fromtimestamp(stat_info.st_mtime).strftime("%Y-%m-%d")
                        mt_str = datetime.fromtimestamp(stat_info.st_mtime).strftime("%H:%M:%S")
                        sz_val = stat_info.st_size if not is_dir else 0
                        dir_path = os.path.dirname(os.path.relpath(entry.path, base_dir))
                        if dir_path == "": dir_path = "\\"
                        if engine == 'ram':
                            results_ram.append({'base': base_dir, 'is_dir': is_dir, 'path': dir_path, 'name': entry.name, 'cdate': cd_str, 'mdate': md_str, 'mtime': mt_str, 'sz': sz_val})
                        else:
                            db_cursor.execute("INSERT INTO files VALUES (?,?,?,?,?,?,?,?)", (base_dir, int(is_dir), dir_path, entry.name, cd_str, md_str, mt_str, sz_val))
                except (PermissionError, OSError, ValueError): continue
        if dest == 'full':
            if engine == 'ram':
                results_ram.sort(key=lambda x: (x['base'], not x['is_dir'], x['path'].lower(), x['name'].lower()))
                return results_ram
            else:
                db_conn.commit()
                return (db_conn, db_path)
        return None

    def logic_redundancy_scan(self, queue_targets, min_size, max_size):
        size_dict = {}
        for base_dir in queue_targets:
            self.stats['current_target'] = base_dir
            for entry in self._fast_scandir(base_dir, recursive=True, skip_sensitive=self._current_skip_sensitive):
                if self.stop_event.is_set(): break
                is_dir = entry.is_dir(follow_symlinks=False)
                if is_dir or entry.is_symlink(): continue
                self.stats['scanned'] += 1
                if self.stats['scanned'] % 100000 == 0: self.ui_queue.put(f"> Scanned {self.stats['scanned']:,} files...")
                try:
                    attrs = entry.stat(follow_symlinks=False).st_file_attributes
                    if bool(attrs & FILE_ATTRIBUTE_REPARSE_POINT): continue
                    if self._current_skip_sensitive and (bool(attrs & FILE_ATTRIBUTE_OFFLINE) or bool(attrs & FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS)):
                        self.stats['targets'][base_dir]['skipped_hidden'] += 1
                        continue
                    if self._current_skip_hidden and bool(attrs & (stat.FILE_ATTRIBUTE_HIDDEN | stat.FILE_ATTRIBUTE_SYSTEM)):
                        self.stats['targets'][base_dir]['skipped_hidden'] += 1
                        continue
                    stat_info = entry.stat(follow_symlinks=False)
                    sz = stat_info.st_size
                    if sz < min_size:
                        self.stats['targets'][base_dir]['skipped_small'] += 1
                        continue
                    if max_size > 0 and sz > max_size:
                        self.stats['targets'][base_dir]['skipped_large'] += 1
                        continue
                    self._update_analytics(base_dir, entry, is_dir, stat_info)
                    if sz not in size_dict: size_dict[sz] = []
                    size_dict[sz].append(entry.path)
                except (PermissionError, OSError): continue
        potential_dupes = {sz: paths for sz, paths in size_dict.items() if len(paths) > 1}
        hash_dict = {}
        chunk_mb_val = self.hash_chunk_mb.get()
        total_to_hash = sum(len(paths) for paths in potential_dupes.values())
        self.stats['total_to_hash'] = total_to_hash
        try: res_cores = int(self.reserved_cores.get())
        except ValueError: res_cores = 2
        sys_cores = os.cpu_count() or 4
        max_workers = max(1, sys_cores - res_cores)
        self.stats['workers'] = max_workers
        if total_to_hash > 0: self.ui_queue.put(f"\n> PARTIAL HASHING {total_to_hash:,} files using {max_workers} cores...")
        hashed_count = 0
        tasks_t2 = [(path, sz, chunk_mb_val) for sz, paths in potential_dupes.items() for path in paths]
        if tasks_t2:
            with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
                results_t2 = executor.map(parallel_partial_hash, tasks_t2, chunksize=100)
                for filepath, sz, h in results_t2:
                    if self.stop_event.is_set():
                        executor.shutdown(wait=False, cancel_futures=True)
                        break
                    hashed_count += 1
                    self.stats['hashed'] = hashed_count
                    if hashed_count % 5000 == 0: self.ui_queue.put(f"  ... Hashed {hashed_count:,} of {total_to_hash:,}...")
                    if h:
                        if h not in hash_dict: hash_dict[h] = []
                        hash_dict[h].append({"path": filepath, "size": sz})
        true_dupes = {h: data for h, data in hash_dict.items() if len(data) > 1}
        if self.run_tier4.get() and true_dupes:
            final_dupes = {}
            full_hash_count = 0
            tasks_t4 = [(item['path'], item) for items in true_dupes.values() for item in items]
            total_to_full_hash = len(tasks_t4)
            self.stats['total_to_full_hash'] = total_to_full_hash
            self.ui_queue.put(f"\n> FULL HASHING {total_to_full_hash:,} files using {max_workers} cores...")
            with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
                results_t4 = executor.map(parallel_full_hash, tasks_t4, chunksize=100)
                for filepath, item, fh in results_t4:
                    if self.stop_event.is_set():
                        executor.shutdown(wait=False, cancel_futures=True)
                        break
                    full_hash_count += 1
                    self.stats['full_hashed'] = full_hash_count
                    if fh:
                        if fh not in final_dupes: final_dupes[fh] = []
                        final_dupes[fh].append(item)
            true_dupes = {h: data for h, data in final_dupes.items() if len(data) > 1}
        for h, items in true_dupes.items():
            self.stats['dupes_found'] += (len(items) - 1)
            self.stats['reclaimable_bytes'] += (items[0]['size'] * (len(items) - 1))
        return true_dupes

    def _get_suggested_filename(self, extension, source_dirs):
        date_stamp = datetime.now().strftime("%Y-%m-%d_%H%M")
        if self.queue_mode.get() == 'lumped' and len(source_dirs) > 1:
            root_name = "Multi-Drive" if len(set(os.path.splitdrive(d)[0].upper() for d in source_dirs)) > 1 else f"{os.path.splitdrive(source_dirs[0])[0].replace(':', '')}_Multi-Folder"
        else:
            drive, tail = os.path.splitdrive(source_dirs[0])
            folder = os.path.basename(os.path.normpath(source_dirs[0]))
            root_name = f"{drive.replace(':', '')}_{folder}" if folder and folder not in ('\\', '/') else f"{drive.replace(':', '')}_Root"
        mode_suffix = "_DuplicateHunter" if self.op_mode.get() == 'redundancy' else "_List"
        
        # --- NEW: Append Depth Level to Filename ---
        try: depth_limit = int(self.max_depth_var.get())
        except ValueError: depth_limit = 0
        depth_suffix = f"_Depth{depth_limit}" if depth_limit > 0 else ""
        
        return f"{root_name}_{date_stamp}{mode_suffix}{depth_suffix}.{extension}"

    def handle_standard_output(self, results, source_dirs, engine):
        sugg_name = self._get_suggested_filename('txt', source_dirs)
        base_path = os.path.join(self.save_directory, sugg_name)
        header_text = self._build_analytics_text(source_dirs, include_header=True)
        try: max_bytes = float(self.chunk_mb.get() or 0) * 1024 * 1024
        except ValueError: max_bytes = 0
        headers = ["Type"]
        if self.include_cdate.get(): headers.append("Created     ")
        if self.include_mdate.get(): headers.append("Modified    ")
        if self.include_time.get(): headers.append("Mod_Time  ")
        if self.include_size.get(): headers.append("Size(bytes) ")
        headers.append("Root")
        if self.include_path.get(): headers.append("Folder_Path")
        if self.include_name.get(): headers.append("File_Name")
        inc_cdate, inc_mdate, inc_time, inc_size, inc_path, inc_name = self.include_cdate.get(), self.include_mdate.get(), self.include_time.get(), self.include_size.get(), self.include_path.get(), self.include_name.get()
        def formatter(row):
            if engine == 'ram': r = row
            else: r = {'base': row[0], 'is_dir': bool(row[1]), 'path': row[2], 'name': row[3], 'cdate': row[4], 'mdate': row[5], 'mtime': row[6], 'sz': row[7]}
            parts = ["DIR " if r['is_dir'] else "FILE"]
            if inc_cdate: parts.append(f"{r['cdate']:<12}")
            if inc_mdate: parts.append(f"{r['mdate']:<12}")
            if inc_time: parts.append(f"{r['mtime']:<10}")
            if inc_size: parts.append(f"{r['sz']:<12,}" if not r['is_dir'] else "---         ")
            parts.append(r['base'])
            if inc_path: parts.append(r['path'])
            if inc_name: parts.append(r['name'])
            return " | ".join(parts)
        if self.output_txt.get():
            iterator = results if engine == 'ram' else results[0].execute("SELECT * FROM files ORDER BY base, is_dir DESC, LOWER(path), LOWER(name)")
            self._write_text_report(base_path, iterator, " | ".join(headers), max_bytes, formatter, header_text)
        if self.output_csv.get():
            base_csv_path = base_path.replace('.txt', '')
            part_num = 1
            current_csv_path = f"{base_csv_path}_Part{part_num:03d}.csv" if max_bytes > 0 else f"{base_csv_path}.csv"
            try:
                f = open(current_csv_path, 'w', encoding='utf-8-sig', newline='')
                writer = csv.writer(f)
                writer.writerow(headers)
                iterator = results if engine == 'ram' else results[0].execute("SELECT * FROM files ORDER BY base, is_dir DESC, LOWER(path), LOWER(name)")
                
                lines_written = 0
                for row in iterator:
                    if self.stop_event.is_set(): break
                    if engine == 'ram': r = row
                    else: r = {'base': row[0], 'is_dir': bool(row[1]), 'path': row[2], 'name': row[3], 'cdate': row[4], 'mdate': row[5], 'mtime': row[6], 'sz': row[7]}
                    c_row = ["DIR" if r['is_dir'] else "FILE"]
                    if inc_cdate: c_row.append(r['cdate'])
                    if inc_mdate: c_row.append(r['mdate'])
                    if inc_time: c_row.append(r['mtime'])
                    if inc_size: c_row.append(r['sz'] if not r['is_dir'] else '')
                    c_row.append(r['base'])
                    if inc_path: c_row.append(r['path'])
                    if inc_name: c_row.append(r['name'])
                    
                    lines_written += 1
                    
                    # Split Logic: Check file size every 5000 rows to maintain high processing speed
                    if lines_written % 5000 == 0:
                        current_bytes = f.tell()
                        if (max_bytes > 0 and current_bytes > max_bytes) or (max_bytes == 0 and current_bytes > (250 * 1024 * 1024)):
                            f.close()
                            if current_csv_path not in self.stats['generated_paths']:
                                self.stats['generated_paths'].append(current_csv_path)
                            part_num += 1
                            current_csv_path = f"{base_csv_path}_Part{part_num:03d}.csv"
                            f = open(current_csv_path, 'w', encoding='utf-8-sig', newline='')
                            writer = csv.writer(f)
                            writer.writerow(headers) # Ensure the new part has the header row
                            
                    writer.writerow(c_row)
                
                f.close()
                if current_csv_path not in self.stats['generated_paths']:
                    self.stats['generated_paths'].append(current_csv_path)
            except Exception as e: 
                self.ui_queue.put(f"[!!!] CSV ERROR: {str(e)}")
                try: f.close() 
                except: pass
        if engine == 'sql':
            try:
                results[0].execute("PRAGMA wal_checkpoint(TRUNCATE);")
                results[0].close(); time.sleep(0.5)
                if os.path.exists(results[1]): os.remove(results[1])
            except Exception as e: self.ui_queue.put(f"Cleanup deferred: {str(e)}")

    def handle_redundancy_output(self, dupes_dict, source_dirs):
        sugg_name = self._get_suggested_filename('txt', source_dirs)
        base_path = os.path.join(self.save_directory, sugg_name)
        header_text = self._build_analytics_text(source_dirs, include_header=True)
        try: max_bytes = float(self.chunk_mb.get() or 0) * 1024 * 1024
        except ValueError: max_bytes = 0
        lines_data = []
        for h, items in dupes_dict.items():
            lines_data.append(f"\n=== MATCH GROUP (Hash: {h[:8]}... | Size: {items[0]['size']:,} bytes) ===")
            for item in items: lines_data.append(f"  -> {item['path']}")
        if self.output_txt.get(): self._write_text_report(base_path, lines_data, "DUPLICATE REPORT", max_bytes, lambda x: x, header_text)

    def _write_text_report(self, base_filepath, data_iterable, col_headers, max_bytes, formatter, top_header_text):
        part_num = 1
        current_bytes = 0
        lines_written = 0 
        base, ext = os.path.splitext(base_filepath)
        current_path = f"{base}_Part{part_num:03d}{ext}" if max_bytes > 0 else base_filepath
        try:
            total_file_bytes = 0 # FIXED IN AUDIT
            buffer = io.StringIO()
            if part_num == 1:
                header_block = f"{top_header_text}\n{'-'*60}\n{col_headers}\n{'-'*60}\n"
            else:
                header_block = f"{col_headers}\n{'-'*60}\n"
            buffer.write(header_block)
            current_bytes += len(header_block.encode('utf-8'))
            total_file_bytes += current_bytes
            for item in data_iterable:
                if self.stop_event.is_set(): break 
                line_str = formatter(item) + '\n'
                line_bytes = line_str.encode('utf-8')
                lines_written += 1
                if lines_written % 5000 == 0: self.engine_status_msg = f"Writing... ({lines_written:,})"
                if max_bytes > 0 and (current_bytes + len(line_bytes)) > max_bytes:
                    with open(current_path, 'w', encoding='utf-8') as f: f.write(buffer.getvalue())
                    self.stats['generated_paths'].append(current_path)
                    part_num += 1; current_path = f"{base}_Part{part_num:03d}{ext}"
                    buffer = io.StringIO()
                    cont_header = f"--- CONTINUED FROM PART {part_num-1:03d} ---\n{col_headers}\n{'-'*60}\n"
                    buffer.write(cont_header); current_bytes = len(cont_header.encode('utf-8'))
                elif max_bytes == 0 and (total_file_bytes + len(line_bytes)) > (250 * 1024 * 1024):
                    with open(current_path, 'a', encoding='utf-8') as f: f.write(buffer.getvalue())
                    if current_path not in self.stats['generated_paths']: self.stats['generated_paths'].append(current_path)
                    part_num += 1; current_path = f"{base}_Part{part_num:03d}{ext}"
                    buffer = io.StringIO()
                    cont_header = f"--- CONTINUED (FAILSAFE SPLIT) ---\n{col_headers}\n{'-'*60}\n"
                    buffer.write(cont_header); current_bytes = len(cont_header.encode('utf-8')); total_file_bytes = current_bytes
                elif max_bytes == 0 and current_bytes > self.internal_flush_limit:
                    with open(current_path, 'a', encoding='utf-8') as f: f.write(buffer.getvalue())
                    buffer = io.StringIO(); current_bytes = 0
                buffer.write(line_str); current_bytes += len(line_bytes); total_file_bytes += len(line_bytes)
            with open(current_path, 'a' if max_bytes == 0 else 'w', encoding='utf-8') as f: f.write(buffer.getvalue())
            if current_path not in self.stats['generated_paths']: self.stats['generated_paths'].append(current_path)
        except Exception as e: self.ui_queue.put(f"[!!!] WRITE ERROR: {str(e)}")

    def display_analytics_dashboard(self):
        # 1. Build text WITH header for the Saved File
        dashboard_text_file = self._build_analytics_text(list(self.stats['targets'].keys()), include_header=True)
        
        # 2. Build text WITHOUT header for the Screen (since we printed it at the start)
        dashboard_text_screen = self._build_analytics_text(list(self.stats['targets'].keys()), include_header=False)
        
        if self.output_dest.get() == 'summary' and self.save_directory:
            s_name = self._get_suggested_filename('txt', list(self.stats['targets'].keys())).replace('_List.txt', '_Summary.txt')
            try:
                # Save the version WITH the header to the drive
                with open(os.path.join(self.save_directory, s_name), 'w', encoding='utf-8') as f: 
                    f.write(dashboard_text_file)
                dashboard_text_screen += f"\n[SYSTEM] Summary exported to: {s_name}"
            except Exception as e: 
                self.ui_queue.put(f"Summary Error: {str(e)}")
                
        # 3. Print the screen version
        self.ui_queue.put("\n" + dashboard_text_screen)
        self.engine_status_msg = "Scan Complete." if not self.stop_event.is_set() else "Aborted."

    def _build_analytics_text(self, targets, include_header=False):
        elapsed = max(0.1, (self.stats['end_time'] or time.time()) - self.stats['start_time'])
        lines = []
        if include_header:
            lines.append(self._get_file_header()) 
            mode_text = "Standard Directory List" if self.op_mode.get() == 'standard' else "Smart Duplicate Hunter"
            engine_text = "In-Memory Engine (RAM)" if self.engine_choice.get() == 'ram' else "SQLite Disk Engine"
            dest_val = self.output_dest.get()
            if dest_val == 'full': dest_text = "Full Reports"
            elif dest_val == 'screen': dest_text = "Summary to Screen Only"
            else: dest_text = "Summary to Screen and TXT"

            skip_hidden_text = "Yes" if self.skip_hidden.get() else "No"
            
            # --- NEW: Retrieve Depth Limit for Dashboard ---
            try: depth_limit = int(self.max_depth_var.get())
            except ValueError: depth_limit = 0
            depth_text = "Unlimited (Fully Recursive)" if depth_limit == 0 else f"{depth_limit} (Stats reflect ONLY scanned levels)"
            
            scan_time = max(0.1, self.stats.get('scan_time_sec', elapsed))
            scan_speed = int(self.stats['scanned'] / scan_time)
            lines.extend(["="*70, f"          COKER'S LIST MAKER {self.version} - ANALYTICS DASHBOARD", "="*70])
            if self.stop_event.is_set(): lines.extend([" *** WARNING: ABORTED. DATA INCOMPLETE. ***", "="*70])
            lines.extend([
                " --- RUN SETTINGS ---", 
                f" Operating Mode:        {mode_text}", 
                f" Processing Engine:     {engine_text}", 
                f" Destination Output:    {dest_text}", 
                f" Max Subfolder Depth:   {depth_text}", # <-- NEW LINE
                f" Skip Hidden/System:    {skip_hidden_text}", 
                f" Skip Files < (KB):     {self.min_size_kb.get()}", 
                f" Skip Files > (MB):     {self.max_size_mb.get()}", 
                "", 
                " --- GLOBAL SUMMARY ---"
            ])
            
            # --- NEW: Add dynamic warning to Global Summary ---
            if depth_limit > 0:
                lines.append(" *** NOTE: Depth limit applied. Volume metrics exclude omitted subfolders. ***")
                
            lines.extend([
                f" Targets Scanned:       {len(targets)} ({', '.join(targets)})", 
                f" Total Processing Time: {elapsed:.2f} seconds"
            ])
            if self.output_dest.get() == 'full': lines.extend([f"   * Phase 1 (Scanning): {self.stats['scan_time_sec']:.2f} seconds", f"   * Phase 2 (Sorting & Export): {self.stats['write_time_sec']:.2f} seconds"])
            lines.extend([f" Total Files Scanned:   {self.stats['scanned']:,}", f" True Scan Speed:       ~{scan_speed:,} files/sec (Phase 1)"])
            if self.op_mode.get() == 'redundancy':
                lines.extend([f" Duplicate Files Found: {self.stats['dupes_found']:,}", f" Space Reclaimable:     {self.stats['reclaimable_bytes'] / (1024**3):.2f} GB"])
                if self.queue_mode.get() == 'lumped' and self.stats.get('true_dupes'):
                    lines.extend(["", " --- TOP 10 LARGEST DUPLICATE GROUPS (GLOBAL) ---"])
                    sorted_dupes = sorted(self.stats['true_dupes'].values(), key=lambda x: x[0]['size'], reverse=True)
                    for idx, items in enumerate(sorted_dupes[:10], 1):
                        lines.append(f"   {idx}. {items[0]['size'] / (1024**2):.2f} MB per file | {len(items)} copies | Reclaimable: {(items[0]['size'] / (1024**2)) * (len(items) - 1):.2f} MB")
                        lines.append(f"      -> {items[0]['path']} (and {len(items)-1} other locations)")
        for t in targets:
            data = self.stats['targets'][t]
            
            lines.extend([
                "",
                f" --- TARGET: {t} ---",
                f" Volume Metrics:   {data['total_files']:,} files | {data['total_size'] / (1024**3):.2f} GB Total Data",
                f" Skipped Files:    {data['skipped_hidden']:,} Hidden/System | {data['skipped_offline']:,} Offline/Cloud",
                f" Filtered Size:    {data['skipped_small']:,} Too Small | {data['skipped_large']:,} Too Large"
            ])
            if data['total_files'] > 0:
                if data['oldest']: dt_str = datetime.fromtimestamp(data['oldest'][0]).strftime("%Y-%m-%d"); lines.append(f" Oldest File:      {dt_str} ({data['oldest'][1]})")
                if data['newest']: dt_str = datetime.fromtimestamp(data['newest'][0]).strftime("%Y-%m-%d"); lines.append(f" Newest File:      {dt_str} ({data['newest'][1]})")
                if data['extensions']:
                    lines.append(" Top 10 Extensions:")
                    for ext, count in data['extensions'].most_common(10): lines.append(f"   * {ext or '<none>'}: {count:,} files ({(count/data['total_files'])*100:.1f}%)")
                if data['top_10']:
                    lines.append(" Top 10 Largest Files:")
                    sorted_top = sorted(data['top_10'], key=lambda x: x[0], reverse=True)
                    for idx, (sz, path) in enumerate(sorted_top, 1): lines.append(f"   {idx}. {path} ({sz / (1024**2):.2f} MB)")
        lines.append("\n" + "="*70)
        return '\n'.join(lines)

    def hard_exit(self):
        try:
            if self.is_running:
                if not messagebox.askyesno("Exit", "Scan in Progress. Force exit?", parent=self): return
            self.stop_event.set()
            for child in multiprocessing.active_children():
                try: child.terminate()
                except: pass
        finally: os._exit(0)

if __name__ == "__main__":
    multiprocessing.freeze_support() 
    app = ListMakerApp()
    app.mainloop()