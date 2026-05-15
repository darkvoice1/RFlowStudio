[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [string]$MessageFile = "commit-message.txt",

    [switch]$Init,
    [switch]$Amend,
    [switch]$NoVerify,
    [switch]$Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Utf8NoBomFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,

        [Parameter(Mandatory = $true)]
        [string]$Content
    )

    $directory = Split-Path -Parent $Path
    if ($directory -and -not (Test-Path -LiteralPath $directory)) {
        New-Item -ItemType Directory -Path $directory | Out-Null
    }

    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($Path, $Content, $utf8NoBom)
}

function Get-CommitTemplateBody {
    param(
        [Parameter(Mandatory = $true)]
        [string]$TemplateMarkdownPath
    )

    $fallback = @'
<type>(<scope>): <summary>

Background:
1. <why this change is needed>
2. <current limitation or risk>

Changes:
1. <change A>
2. <change B>
3. <change C>

Tests:
1. <new or updated tests>
2. <covered scenarios>
3. <result and command>

Benefits:
1. <business value>
2. <engineering value>
3. <reuse value>
'@

    if (-not (Test-Path -LiteralPath $TemplateMarkdownPath)) {
        return $fallback
    }

    $templateMarkdown = [System.IO.File]::ReadAllText(
        $TemplateMarkdownPath,
        [System.Text.Encoding]::UTF8
    )
    $match = [regex]::Match(
        $templateMarkdown,
        '(?s)```text\s*(.*?)```'
    )
    if (-not $match.Success) {
        return $fallback
    }

    return $match.Groups[1].Value.Trim() + [Environment]::NewLine
}

function Resolve-RepoPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$RepoRoot,

        [Parameter(Mandatory = $true)]
        [string]$PathText
    )

    if ([System.IO.Path]::IsPathRooted($PathText)) {
        return $PathText
    }

    return [System.IO.Path]::GetFullPath((Join-Path $RepoRoot $PathText))
}

$scriptDir = Split-Path -Parent $PSCommandPath
$repoRoot = [System.IO.Path]::GetFullPath((Join-Path $scriptDir ".."))
$messageFilePath = Resolve-RepoPath -RepoRoot $repoRoot -PathText $MessageFile
$templateFile = Get-ChildItem -LiteralPath $repoRoot -File |
    Where-Object { $_.Name -like "*commit*.md" } |
    Select-Object -First 1
$templateMarkdownPath = if ($templateFile) { $templateFile.FullName } else { "" }

if ($Init) {
    if ((Test-Path -LiteralPath $messageFilePath) -and -not $Force) {
        throw "Message file already exists: $messageFilePath. Use -Force to overwrite it."
    }

    $templateBody = Get-CommitTemplateBody -TemplateMarkdownPath $templateMarkdownPath
    Write-Utf8NoBomFile -Path $messageFilePath -Content $templateBody
    Write-Output "Created UTF-8 commit message template: $messageFilePath"
    Write-Output "After editing, run: powershell -ExecutionPolicy Bypass -File .\scripts\commit-msg.ps1 -MessageFile `"$MessageFile`""
    exit 0
}

if (-not (Test-Path -LiteralPath $messageFilePath)) {
    throw "Message file not found: $messageFilePath. Run with -Init first or prepare a UTF-8 text file."
}

$messageContent = [System.IO.File]::ReadAllText(
    $messageFilePath,
    [System.Text.Encoding]::UTF8
)
if ([string]::IsNullOrWhiteSpace($messageContent)) {
    throw "Message file is empty: $messageFilePath"
}

& git -C $repoRoot config --local i18n.commitEncoding utf-8
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

& git -C $repoRoot config --local i18n.logOutputEncoding utf-8
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

$normalizedMessagePath = Join-Path $repoRoot ".git\commit-message.utf8.txt"
Write-Utf8NoBomFile -Path $normalizedMessagePath -Content $messageContent

try {
    $gitArgs = @("-C", $repoRoot, "commit")
    if ($Amend) {
        $gitArgs += "--amend"
    }
    if ($NoVerify) {
        $gitArgs += "--no-verify"
    }
    $gitArgs += @("-F", $normalizedMessagePath)

    & git @gitArgs
    exit $LASTEXITCODE
}
finally {
    if (Test-Path -LiteralPath $normalizedMessagePath) {
        Remove-Item -LiteralPath $normalizedMessagePath -Force
    }
}
