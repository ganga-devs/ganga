import sys
from ganga.GangaCore.Utility.Deprecation import generate_deprecation_json_report

root = "./ganga"
r = generate_deprecation_json_report(root)
if len(sys.argv) > 1:
    f = open(sys.argv[1], 'w')
    f.write(r)
else:
    print(r)
