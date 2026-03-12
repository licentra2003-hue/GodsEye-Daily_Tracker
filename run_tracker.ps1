param(
  [switch]$Once = $true
)

$ErrorActionPreference = "Stop"

python -m pip install -r "$PSScriptRoot\requirements.txt"

if ($Once) {
  python "$PSScriptRoot\main.py" --once
} else {
  python "$PSScriptRoot\main.py"
}
