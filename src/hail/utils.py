import os
import hail as hl

try:
    hc = hl.HailContext()
    hl.init()
except Exception:
    pass


def vcf_path_to_mt(vcf_path: str, extension: str = '.mt') -> hl.MatrixTable:
    mt_path = os.path.basename(vcf_path).split('.')[0] + extension
    if not os.path.exists(mt_path):
        mt = hl.import_vcf(vcf_path)
        mt.write(mt_path)
    else:
        mt = hl.read_matrix_table(mt_path)
    return mt
