import shlex
import subprocess

class SplitMerge(object):

    MKVMERGE = 'mkvmerge'
    SPLIT_TEMPLATE = MKVMERGE + ' -A -S -M -B -T --no-chapters --no-global-tags --split %(split)s -o %(output)s %(input)s'
    MERGE_TEMPLATE = MKVMERGE + ' -D %(original)s %(files_to_merge)s -o %(output)s'

    def split(self, input_file, output_files, split_time):
        command = SplitMerge.SPLIT_TEMPLATE % ({
            "input": input_file,
            "output": output_files,
            "split": split_time
            })

        returncode = subprocess.call(shlex.split(command))
        return True if returncode == 0 else False

    def merge(self, original_file, files_to_merge, output_file):
        merge_files = ''
        for file in files_to_merge:
            merge_files += ' -T --no-global-tags %s +' % file
        if merge_files[-1] == '+':
            merge_files = merge_files[:-2:]

        command = SplitMerge.MERGE_TEMPLATE % ({
            "original": original_file,
            "files_to_merge": merge_files,
            "output": output_file
            })

        returncode = subprocess.call(shlex.split(command))
        return True if returncode == 0 else False