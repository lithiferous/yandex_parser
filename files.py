import csv
import os
import pandas as pd

from datetime import datetime as dt
from glob import glob
from shutil import make_archive, rmtree

class LogsDef:
    def __init__(self, current_date, filepath, rtb = None, outname = None):
        self.current_date = current_date
        self.filepath = filepath
        self.outname = outname if outname is not None else self.get_outname()
        self.rtb = rtb if rtb is not None else "mytarget"
        
        assert dt.strptime(current_date, '%Y-%m-%d'), "Date format should be yyyy.mm.dd"
        assert os.path.isdir(filepath) == True, "Absolute path to segment folder is incorrect"
        if outname:
            assert len(outname) > 255, "Out archive name should be less than 255 chr"
    

    def get_outname(self):
        media = self.filepath.split("\\")[-2]
        brand = self.filepath.split("\\")[-1].split("_")[-1]
        return "_".join([self.current_date, media, brand])
            
           
class GetAttribution:
    """Read, upload files"""        
    def __init__(self, segments):
        self.segments = segments
        self.current_date = segments.current_date
        self.filepath = segments.filepath
        self.outname = segments.outname
        self.rtb = segments.rtb
        self.zippath = os.path.join(self.filepath + '\\logs', self.outname)

    def __str__(self):
        return """Path in: %s
                  Wrote out: %s
                  """ % (self.filepath,
                         self.zippath)

    def chunkify_to_zip(self, seq, filename):
        """
        Write sequence of hash to files by parts
        :param seq: seq of hashes
        :param filename:
        :return:
        """
        n = int(seq.shape[0] / 348841) + 1
        for i in range(n):
            file_part = filename.replace('.csv', '_{}.csv'.format(i))
            seq[i::n].to_csv(file_part, index=False)

    def get_ids_seq(self, files_path: str, current_date: str) -> pd.DataFrame:
        """
        Read file with hash
        :param files_path: list of str, path to files with ids of one type (mob or mail)
        :param current_date: str, date 'dd.mm.yyyy' for ex. '12.01.2018'
        :return: DataFrame columns = (hash, current_date)
        """
        df = pd.concat(
            [pd.read_csv(open(f), header=None, names=['id'])
            for f in files_path]
        )
        t = df.drop_duplicates(). \
            reset_index(drop=True)
        t['date'] = current_date
        return t

    def write_files(self):
        """
        Iterate over files, founed by mask, and create mail.ru files to upload
        :param files_mask:
        :param out_dir:
        :param current_date: str, date 'dd.mm.yyyy' for ex. '12.01.2018'
        :return:
        """    
        out_dir = self.filepath + '\\logs\\tmp'
        print(out_dir)
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        if self.rtb == "vk":
            files_mask = r'\**\vk\target*.csv'
        else:
            files_mask = r'\**\mail.ru\target*.csv'

        files_mask = self.filepath + files_mask
        files_filtered = [x for x in glob(files_mask, recursive=True)
                        if '_old' not in x and 'segment_mapping' not in x and 'target' in x and 'shops' not in x]

        for file_type in ['email', 'mob']:
            files_path = [x for x in files_filtered if file_type in x]
            print('\n'.join([os.path.basename(x) for x in files_path]))
            t = self.get_ids_seq(files_path, self.current_date)
            filename = os.path.join(out_dir, file_type + '.csv')
            self.chunkify_to_zip(t, filename)
        
        
        make_archive(self.zippath, 'zip', out_dir)
        rmtree(out_dir)

        return self