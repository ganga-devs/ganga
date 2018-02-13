#!/bin/bash
#Setup the release
echo $GIT_BRANCH

BRANCHNAME=$(echo ${GIT_BRANCH} | cut -d "/" -f 2)
VERSION=$(echo ${GIT_BRANCH} | cut -d "-" -f 2)

#We start on a commit so checkout the branch
git checkout -b $BRANCHNAME

git branch

git config --global push.default current

echo $BRANCHNAME
echo $VERSION

echo "Checking requested release version string"
#First remove all local tags and get the ones from the remote. This is in case of imperfect clean up
git tag -d $(git tag)
git fetch --tags

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

echo "Sorting release notes"

#Setting version and date on release notes
sed --in-place "s/@VERSION@/${VERSION} (`date '+%Y\/%m\/%d'`)/g" ganga/GangaRelease/ReleaseNotes

#Copying release notes
git mv ganga/GangaRelease/ReleaseNotes ganga/GangaRelease/ReleaseNotes-${VERSION}

#Creating new release notes from template
cat << EOF > ganga/GangaRelease/ReleaseNotes
**************************************************************************************************************
@VERSION@


--------------------------------------------------------------------------------------------------------------
ganga/ganga/Ganga
--------------------------------------------------------------------------------------------------------------
* ...

**************************************************************************************************************
EOF
git add ganga/GangaRelease/ReleaseNotes
git add ganga/GangaRelease/ReleaseNotes-${VERSION}

#Committing changes
git commit -m "Updating release notes"

echo "Changing version numbers"

#Setting version number and turn off DEV flag
sed --in-place "s/^_gangaVersion = .*/_gangaVersion = '${VERSION}'/g" ganga/GangaCore/__init__.py
sed --in-place "s/^_gangaVersion = .*/_gangaVersion = '${VERSION}'/g" ./setup.py
sed --in-place "s/^_development = .*/_development = False/g" ganga/GangaCore/__init__.py
git add ganga/GangaCore/__init__.py ./setup.py

#Committing changes
git commit -m "Setting release number"
echo "Creating tag $VERSION"
#Now create a tag and fire it at github
git tag -a $VERSION -m "Release ${VERSION}"

echo "Pushing to origin"
git push --tags

#Now send the release notes to github - need some python magic
echo "Creating new release on github"
function sendReleaseNotes {
version=$VERSION apitoken=$GITHUBAPITOKEN python - <<END
import requests
import json
import os

version = os.environ.get('version')

changelog = open('ganga/GangaRelease/ReleaseNotes-'+version, 'r').readlines()
changelog = changelog[4:-2]  # Strip headings...?
changelog = ''.join(changelog)

release = {
  'tag_name': version,
  'target_commitish': 'master',
  'name': version,
  'body': changelog,
  'draft': False,
  'prerelease': False
}

r = requests.post('https://api.github.com/repos/ganga-devs/ganga/releases', data=json.dumps(release), headers={'Authorization':'token %s'  % os.environ.get('apitoken')})

r.raise_for_status()
END
}

sendReleaseNotes

#Below is the necessaries for the pypi upload. Maybe best done somewhere else but if this is running in a virtual env then probably fine.

pip install --upgrade pip
pip install --upgrade twine

cat << EOF > ~/.pypirc
[distutils]
index-servers =
    pypi
    testpypi

[testpypi]
#The repository line is apparently outdated now
#repository = https://pypi.python.org/pypi/
repository = htpps://test.pypi.org/legacy/
username: $PYPI_USER
password: $PYPI_PASSWORD
EOF
echo "uploading to test pypi"
python setup.py register
python setup.py sdist
twine upload --skip-existing --respository testpypi dist/ganga-*.tar.gz

rm dist/ganga-*.tar.gz
rm ~/.pypirc

#Now the release is sorted we can set the development flag again and push the changes back to the release branch!
sed --in-place "s/^_development = .*/_development = True/g" ganga/GangaCore/__init__.py
git add ganga/GangaCore/__init__.py

git commit -m "setting development flag"
git push origin ${BRANCHNAME}

#All done!
