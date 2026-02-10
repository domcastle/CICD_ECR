#!/bin/bash
find /tmp -name "tmp*.mp4" -type f -mmin +60 -delete
find /tmp -name "caption.*.txt" -type f -mmin +60 -delete
echo "[INFO] Cleanup completed."