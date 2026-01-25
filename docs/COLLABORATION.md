# Team Collaboration Guide

---

## Daily Workflow

**1. Pull**
```bash
git pull origin main
```

**2. Work**
- Edit files, create notebooks, write code

**3. Push**
```bash
git add .
git commit -m "Describe your changes"
git push origin main
```

---

## Example

```bash
git pull origin main

# Make your changes...

git add .
git commit -m "Add data exploration notebook"
git push origin main
```

---

## Tips

- ✅ Always pull before starting work
- ✅ Use clear commit messages
- ✅ Commit often
- ❌ Don't force push (`git push -f`)

---

## Fix Merge Conflicts

If `git pull` shows a conflict:
1. Open the file with conflict markers `<<<<<<<` `=======` `>>>>>>>`
2. Edit to keep what you want
3. Remove the markers
4. Save and push:
   ```bash
   git add .
   git commit -m "Fix conflict"
   git push origin main
   ```
