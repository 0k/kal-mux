# -*- mode: shell-script -*-

##
## Generate changelog and append to README.md
## (runs after org-to-md conversion)
##
## Note: gitchangelog with mustache("markdown") outputs:
##   # Changelog, ## version, ### section
## We shift heading levels by 1 (# -> ##) so changelog integrates
## properly with README structure (h1 = title, h2 = sections).
##

depends gitchangelog

if [ -f README.md ] && [ -f .gitchangelog.rc ]; then
    changelog=$(gitchangelog 2>/dev/null)
    if [ -n "$changelog" ]; then
        # Shift heading levels: # -> ##, ## -> ###, etc.
        changelog=$(echo "$changelog" | sed 's/^#/##/')
        # Append changelog to README.md
        echo "" >> README.md
        echo "$changelog" >> README.md
        echo "Appended changelog to README.md" >&2
    else
        echo "Warning: gitchangelog produced no output" >&2
    fi
fi
