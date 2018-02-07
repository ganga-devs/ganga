#!/bin/bash
#Setup the release
#VERSION=$(echo $(git describe) | cut -d "-" -f 1)
VERSION=1.1.1

echo $VERSION

echo "Checking requested release version string"

#Check if the requested version is "x.y.z"
if [[ ! "${VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "ERROR: Version string did not match \"^[0-9]+\.[0-9]+\.[0-9]+$\""
    exit 1
fi

#Check if the requested version has already been released
tag=$(git ls-remote --tags origin ${VERSION})
if [[ $? != 0 ]]; then
    echo "ERROR: Command \"git ls-remote --tags origin ${VERSION}\" failed"
elif [[ $tag ]]; then
    echo "ERROR: Version string already found"
    exit 1
fi

#Setting version and date on release notes
sed --in-place "s/@VERSION@/${VERSION} (`date '+%Y\/%m\/%d'`)/g" release/ReleaseNotes

#Copying release notes
git mv release/ReleaseNotes release/ReleaseNotes-${VERSION}

#Creating new release notes from template
cat << EOF > release/ReleaseNotes
**************************************************************************************************************
@VERSION@


--------------------------------------------------------------------------------------------------------------
ganga/python/Ganga
--------------------------------------------------------------------------------------------------------------
* ...

**************************************************************************************************************
EOF
git add release/ReleaseNotes
git add release/ReleaseNotes-${VERSION}

#Committing changes
git commit -m "Updating release notes"

#Setting version number and turn off DEV flag
#sed --in-place "s/^_gangaVersion = .*/_gangaVersion = '\$Name: ${VERSION} \$'/g" python/Ganga/__init__.py
sed --in-place "s/^_gangaVersion = .*/_gangaVersion = '${VERSION}'/g" python/GangaCore/__init__.py
sed --in-place "s/^_gangaVersion = .*/_gangaVersion = '${VERSION}'/g" ./setup.py
sed --in-place "s/^_development = .*/_development = False/g" python/GangaCore/__init__.py
git add python/GangaCore/__init__.py ./setup.py

#Committing changes
git commit -m "Setting release number"
