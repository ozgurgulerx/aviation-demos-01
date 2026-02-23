
import pathlib, textwrap
target = pathlib.Path('/Users/ozgurguler/Developer/Projects/aviation-demos-01/scripts/upload_runner.py')
target.write_text(textwrap.dedent(open('/Users/ozgurguler/Developer/Projects/aviation-demos-01/scripts/_runner_template.py').read()))
print("Written")
