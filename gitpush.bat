@echo off
REM ============================
REM Simple Force Push Script
REM ============================

cd /d "C:\Users\Nilan\OneDrive\Documents\CruzeBOT\vercel-dashboard"

REM Stage all changes
git add .

REM Commit all changes with a default message
git commit -m "Auto commit"

REM Force push to main
git push -u origin main --force

echo Done!
pause