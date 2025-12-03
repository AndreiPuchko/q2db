# --- 1. Bump version ---
poetry version patch
py c:\Users\andre\Desktop\dev\utils\write_version\write_version.py

# --- 2. Build & Publish to PyPI ---
poetry build
poetry publish

# --- 3. Read version ---
$version = Get-Content q2report/version.py 
$version = ($version | Select-String '__version__\s*=\s*"(.+)"').Matches[0].Groups[1].Value

Write-Host "Version: $version"

# --- 4. Commit changes ---
git add .

$commitMessage = "release: v$version"
git commit -m $commitMessage 2>$null

if ($LASTEXITCODE -eq 0) {
    Write-Host "Changes committed: $commitMessage"
    git push
    Write-Host "Changes pushed to origin."
} else {
    Write-Host "No changes to commit."
}

# --- 5. Create git tag ---
$tag = "v$version"
$existingTag = git tag --list $tag

if ($existingTag) {
    Write-Host "Tag $tag already exists. Skipping tag creation."
} else {
    git tag $tag
    Write-Host "Tag $tag created."
}

git push origin $tag
Write-Host "Tag $tag pushed to remote."