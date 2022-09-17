--loading and parsing logs

logs = LOAD '/user/uti/projet/log.tsv' USING PigStorage('\t') 
AS 
(col_0:chararray,col_1:chararray,col_2:chararray,col_3:chararray,col_4:chararray,
col_5:chararray,col_6:chararray,col_7:chararray,col_8:chararray,col_9:chararray,
col_10:chararray,col_11:chararray,col_12:chararray,col_13:chararray,col_14:chararray,
col_15:chararray,col_16:chararray,col_17:chararray,col_18:chararray,col_19:chararray,
col_20:chararray,col_21:chararray,col_22:chararray,col_23:chararray,col_24:chararray,
col_25:chararray,col_26:chararray,col_27:chararray,col_28:chararray,col_29:chararray,
col_30:chararray,col_31:chararray,col_32:chararray,col_33:chararray,col_34:chararray,
col_35:chararray,col_36:chararray,col_37:chararray,col_38:chararray,col_39:chararray,
col_40:chararray,col_41:chararray,col_42:chararray,col_43:chararray,col_44:chararray,
col_45:chararray,col_46:chararray,col_47:chararray,col_48:chararray,col_49:chararray,
col_50:chararray,col_51:chararray,col_52:chararray,col_53:chararray,col_54:chararray,
col_55:chararray,col_56:chararray,col_57:chararray,col_58:chararray,col_59:chararray,
col_60:chararray,col_61:chararray,col_62:chararray,col_63:chararray,col_64:chararray,
col_65:chararray,col_66:chararray,col_67:chararray,col_68:chararray,col_69:chararray,
col_70:chararray,col_71:chararray,col_72:chararray,col_73:chararray,col_74:chararray,
col_75:chararray,col_76:chararray,col_77:chararray,col_78:chararray,col_79:chararray,
col_80:chararray,col_81:chararray,col_82:chararray,col_83:chararray,col_84:chararray,
col_85:chararray,col_86:chararray,col_87:chararray,col_88:chararray,col_89:chararray,
col_90:chararray,col_91:chararray,col_92:chararray,col_93:chararray,col_94:chararray,
col_95:chararray,col_96:chararray,col_97:chararray,col_98:chararray,col_99:chararray,
col_100:chararray,col_101:chararray,col_102:chararray,col_103:chararray,col_104:chararray,
col_105:chararray,col_106:chararray,col_107:chararray,col_108:chararray,col_109:chararray,
col_110:chararray,col_111:chararray,col_112:chararray,col_113:chararray,col_114:chararray,
col_115:chararray,col_116:chararray,col_117:chararray,col_118:chararray,col_119:chararray,
col_120:chararray,col_121:chararray,col_122:chararray,col_123:chararray,col_124:chararray,
col_125:chararray,col_126:chararray,col_127:chararray,col_128:chararray,col_129:chararray,
col_130:chararray,col_131:chararray,col_132:chararray,col_133:chararray,col_134:chararray,
col_135:chararray,col_136:chararray,col_137:chararray,col_138:chararray,col_139:chararray,
col_140:chararray,col_141:chararray,col_142:chararray,col_143:chararray,col_144:chararray,
col_145:chararray,col_146:chararray,col_147:chararray,col_148:chararray,col_149:chararray,
col_150:chararray,col_151:chararray,col_152:chararray,col_153:chararray,col_154:chararray,
col_155:chararray,col_156:chararray,col_157:chararray,col_158:chararray,col_159:chararray,
col_160:chararray,col_161:chararray,col_162:chararray,col_163:chararray,col_164:chararray,
col_165:chararray,col_166:chararray,col_167:chararray,col_168:chararray,col_169:chararray,
col_170:chararray,col_171:chararray,col_172:chararray,col_173:chararray,col_174:chararray,
col_175:chararray,col_176:chararray,col_177:chararray,col_178:chararray);

logs1 = FOREACH logs GENERATE col_1 AS logdate, 
col_7 AS ip, col_12 AS url, col_13 AS swid, col_49 AS city, col_50 AS country, col_52 AS state;

logs2 = FOREACH logs1 GENERATE logdate, url, ip, city, UPPER(state) AS state, 
UPPER(country) AS country, swid;


store logs2 into '/user/uti/clickstream/pig_out_parsed_log';


--load products
products = LOAD '/user/uti/projet/products.tsv' 
USING PigStorage('\t') AS (url:chararray,category:chararray);

store products into '/user/uti/clickstream/pig_out_products';


--load users

users = LOAD '/user/uti/projet/users.tsv' 
USING PigStorage('\t') AS (SWID:chararray,BIRTH_DT:chararray,GENDER_CD:chararray);
store users into '/user/uti/clickstream/pig_out_users';

