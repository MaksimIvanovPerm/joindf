#! /usr/bin/env python3

import os
import sys
import ast
### https://docs.python.org/3/library/argparse.html#argparse.ArgumentParser ####
import argparse
################################################################################
import pandas as pd
# https://docs.python.org/3/library/configparser.html
import configparser
import logging

base_dir=os.path.dirname(sys.argv[0])
default_config=os.path.join(base_dir, "joindf.conf")
default_loglevel=logging.ERROR
logging.basicConfig(level=default_loglevel, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger()

parser=argparse.ArgumentParser(description='Join two csv-like datafiles by given column;')
parser.add_argument('-f1',action='append',nargs=1,required=True,help='Requeried, one of two csv-liked datafile with data for joining',dest='FILE1')
parser.add_argument('-f2',action='append',nargs=1,required=True,help='Required, one of two csv-liked datafile with data for joining',dest='FILE2')
parser.add_argument('-c',action='append',nargs=1,required=False,help='Config-file, default is: joindf.conf',dest='CFG',)
#>>> parser.parse_args('-f1 /tmp/data1.dat -f2 /tmp/data1.dat -f1 /tmp/data3.dat'.split())
#Namespace(FILE1=['/tmp/data3.dat'], fILE2=['/tmp/data1.dat'])
args=parser.parse_args()
if len(args.FILE1)>1:
	parser.error('f1 argument was setted to list of multiple values; it should be one string value;')

if len(args.FILE2)>1:
        parser.error('f2 argument was setted to list of multiple values; it should be one string value;')

if args.CFG is not None:
	#print ("CFG has been set (value is %s)" % (args.CFG))
	if len(args.CFG)>1:
		parser.error('c argument was setted to list of multiple values; it should be one string value;')
	cfg=args.CFG[0][0]
else:
	#print("CFG has not been setted")
	#args.CFG=str(default_config)
	cfg=str(default_config)

#print ("getcwd: %s" %(os.getcwd()))
#print ("sys.argv[0]: %s" %( os.path.dirname(sys.argv[0]) ))

#for k in args.__dict__:
#  print("%s %s %s" %(k, args.__dict__[k], type(args.__dict__[k])))
f1=args.FILE1[0][0]
f2=args.FILE2[0][0]

logger.info("file1:\t%s\t%s" %(f1, type(f1)))
logger.info("file2:\t%s\t %s" %(f2, type(f2)))
logger.info("config:\t%s\t%s" %(cfg, type(cfg)))

if os.path.isfile(f1):
        logger.info("Ok %s is regular file" %(f1))
else:
        logger.critical("Err: %s is not regular file" %(f1))
        sys.exit(1)

if os.path.isfile(f2):
        logger.info("Ok %s is regular file" %(f2))
else:
        logger.critical("Err: %s is not regular file" %(f2))
        sys.exit(1)

if os.path.isfile(cfg):
        logger.info("Ok %s is regular file" %(cfg))
else:
        logger.critical("Err: %s is not regular file" %(cfg))
        sys.exit(1)

#https://stackoverflow.com/questions/8884188/how-to-read-and-write-ini-file-with-python3
config = configparser.ConfigParser()
config.read(cfg)
for section in config.sections():
	for (v_key, v_val) in config.items(section):
		logger.info("%s\t%s\t%s" %(section, v_key, v_val))

logger.setLevel(config.get('GENERAL','loglevel',fallback=default_loglevel))
logger.info("Try to read settings for processing of join-output")
try:
	v_resultfile=ast.literal_eval(config.get('JOINSETTINGS','file4result'))
except:
	logger.critical("result file should be settet as option 'file4result' in 'JOINSETTINGS' section of %s" % (cfg))
	sys.exit(1)
try:
	v_result_filedsep=ast.literal_eval(config.get('JOINSETTINGS','filedsep'))
except:
	logger.critical("field-separator symbol of rows in result file should be setted as 'filedsep' in 'JOINSETTINGS' in %s" % (cfg))
	sys.exit(1)
try:
	v_result_eol=ast.literal_eval(config.get('JOINSETTINGS','lineterm'))
except:
	logger.critical("EOL-symbol of rows in result file should be setted as 'lineterm' in 'JOINSETTINGS' in %s" % (cfg))
	sys.exit(1)
try:
	v_result_decsep=ast.literal_eval(config.get('JOINSETTINGS','decimalsep'))
except:
	logger.critical("decimal-separator symbol of float-number fields in result file should be setted as 'decimalsep' in 'JOINSETTINGS' in %s" % (cfg))
	sys.exit(1)


logger.info("Try to parse and read first input %s" % (f1))
try:
	v_sep=ast.literal_eval(config.get('F1','delimiter'))
except:
	logger.critical("can not read from %s conf for symbol-delimiter for %s; It should be set in F1-section, as 'delimiter' option" % (cfg, f1))
	sys.exit(1)

try:
	v_header=config.getint('F1','header_line_num')
except:
	logger.critical("can not read from %s conf for number of header-line for %s; It should be set in F1-section, as 'header_line_num' option" % (cfg, f1))
	sys.exit(1)

try:
	v_decsep=ast.literal_eval(config.get('F1','decimal_sep'))
except:
	logger.critical("can not read from %s conf for decimal-sep symbol for %s; It should be set in F1-section, as 'decimal_sep' option" % (cfg, f1))
	sys.exit(1)

try:
	df1=pd.read_csv(f1, sep=v_sep, header=v_header, index_col=False, decimal=v_decsep )
	logger.info("size: %d ndim: %d" %(df1.size, df1.ndim))
except:
	logger.critical("can not parse date from %s" % (f1))
	sys.exit(1)

logger.info("Try to parse and read first input %s" % (f2))
try:
        v_sep=ast.literal_eval(config.get('F2','delimiter'))
except:
        logger.critical("can not read from %s conf for symbol-delimiter for %s; It should be set in F2-section, as 'delimiter' option" % (cfg, f2))
        sys.exit(1)

try:
        v_header=config.getint('F2','header_line_num')
except:
        logger.critical("can not read from %s conf for number of header-line for %s; It should be set in F2-section, as 'header_line_num' option" % (cfg, f2))
        sys.exit(1)

try:
        v_decsep=ast.literal_eval(config.get('F2','decimal_sep'))
except:
        logger.critical("can not read from %s conf for decimal-sep symbol for %s; It should be set in F2-section, as 'decimal_sep' option" % (cfg, f2))
        sys.exit(1)

try:
	df2=pd.read_csv(f2, sep=v_sep, header=v_header, index_col=False, decimal=v_decsep )
	logger.info("size: %d ndim: %d" %(df2.size, df2.ndim))
except:
	logger.critical("can not parse date from %s" % (f2))
	sys.exit(1)

try:
	v_jointype=ast.literal_eval(config.get('JOINSETTINGS','jointype'))
except:
	logger.critical("can not read from %s conf for join-type; It should be set in JOINSETTINGS-section, as 'jointype' option" % (cfg))
	sys.exit(1)
try:
	v_on=ast.literal_eval(config.get('JOINSETTINGS','joinbycolumn'))
except:
	logger.critical("can not read from %s conf for join-column; It should be set in JOINSETTINGS-section, as 'joinbycolumn' option" % (cfg))
	sys.exit(1)

logger.info("Try to perform merge of read datasets")
try:
	js=df1.merge(df2,how=v_jointype, on=v_on,suffixes=('_df1','_df2'))
except:
	logger.critical("can not perform join of datasets")
	sys.exit(1)

logger.info("Try to save result of merge as %s" % (v_resultfile))
js.to_csv(v_resultfile, sep=v_result_filedsep, na_rep="", header=True, index=False, line_terminator=v_result_eol, decimal=v_result_decsep)
logger.info("size: %d ndim: %d" %(js.size, js.ndim))

