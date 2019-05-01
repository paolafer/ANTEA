import sqlite3
import numpy as np
import pandas as pd
import os
from operator  import itemgetter
from functools import lru_cache


class DetDB:
    petalo = os.environ['ANTEADIR'] + '/database/localdb.PETALODB.sqlite3'

def tmap(*args):
    return tuple(map(*args))

def get_db(db):
    return getattr(DetDB, db, db)


@lru_cache(maxsize=10)
def DataSiPM(db_file, run_number=1e5, conf_label='P7R195Z140mm'):

    conn = sqlite3.connect(get_db(db_file))

    sql = '''select pos.SensorID, map.ElecID "ChannelID",
case when msk.SensorID is NULL then 1 else 0 end "Active",
X, Y, Z, Centroid "adc_to_pes", Sigma, PhiNumber, ZNumber
from ChannelPosition{0} as pos INNER JOIN ChannelGain{0} as gain
ON pos.SensorID = gain.SensorID INNER JOIN ChannelMapping{0} as map
ON pos.SensorID = map.SensorID INNER JOIN ChannelMatrix{0} as mtrx
ON pos.SensorID = mtrx.SensorID LEFT JOIN
(select * from ChannelMask{0} where MinRun <= {1} and {1} <= MaxRun) as msk
where pos.MinRun <= {1} and {1} <= pos.MaxRun
and gain.MinRun <= {1} and {1} <= gain.MaxRun
and mtrx.MinRun <= {1} and {1} <= mtrx.MaxRun
order by pos.SensorID
'''.format(conf_label, abs(run_number))
    data = pd.read_sql_query(sql, conn)
    conn.close()

    ## Add default value to Sigma for runs without measurement
    if not data.Sigma.values.any():
        data.Sigma = 2.24

    return data

def ProbabilityList(db_file, run_number=1e5, conf_label='P7R195Z140mm'):

    conn = sqlite3.connect(get_db(db_file))

    sql = '''select PointID, Prob0, Prob1, Prob2, Prob3, Prob4, Prob5, Prob6, Prob7, Prob8, Prob9, Prob10, Prob11, Prob12, Prob13, Prob14, Prob15, Prob16, Prob17, Prob18, Prob19, Prob20, Prob21, Prob22, Prob23, Prob24, Prob25, Prob26, Prob27, Prob28, Prob29, Prob30, Prob31, Prob32, Prob33, Prob34, Prob35, Prob36, Prob37, Prob38, Prob39, Prob40, Prob41, Prob42, Prob43, Prob44, Prob45, Prob46, Prob47, Prob48, Prob49, Prob50, Prob51, Prob52, Prob53, Prob54, Prob55, Prob56, Prob57, Prob58, Prob59, Prob60, Prob61, Prob62, Prob63, Prob64, Prob65, Prob66, Prob67, Prob68, Prob69, Prob70, Prob71, Prob72, Prob73, Prob74, Prob75, Prob76, Prob77, Prob78, Prob79, Prob80, Prob81, Prob82, Prob83, Prob84, Prob85, Prob86, Prob87, Prob88, Prob89, Prob90, Prob91, Prob92, Prob93, Prob94, Prob95, Prob96, Prob97, Prob98, Prob99, Prob100, Prob101, Prob102, Prob103, Prob104, Prob105, Prob106, Prob107, Prob108, Prob109, Prob110, Prob111, Prob112, Prob113, Prob114, Prob115, Prob116, Prob117, Prob118, Prob119, Prob120, Prob121, Prob122, Prob123, Prob124, Prob125, Prob126, Prob127, Prob128, Prob129, Prob130, Prob131, Prob132, Prob133, Prob134, Prob135, Prob136, Prob137, Prob138, Prob139, Prob140, Prob141, Prob142, Prob143, Prob144, Prob145, Prob146, Prob147, Prob148, Prob149, Prob150, Prob151, Prob152, Prob153, Prob154, Prob155, Prob156, Prob157, Prob158, Prob159, Prob160, Prob161, Prob162, Prob163, Prob164, Prob165, Prob166, Prob167, Prob168, Prob169, Prob170, Prob171, Prob172, Prob173, Prob174, Prob175, Prob176, Prob177, Prob178, Prob179, Prob180, Prob181, Prob182, Prob183, Prob184, Prob185, Prob186, Prob187, Prob188, Prob189, Prob190, Prob191, Prob192, Prob193, Prob194, Prob195, Prob196, Prob197, Prob198, Prob199, Prob200, Prob201, Prob202, Prob203, Prob204, Prob205, Prob206, Prob207, Prob208, Prob209, Prob210, Prob211, Prob212, Prob213, Prob214, Prob215, Prob216, Prob217, Prob218, Prob219, Prob220, Prob221, Prob222, Prob223, Prob224, Prob225, Prob226, Prob227, Prob228, Prob229, Prob230, Prob231, Prob232, Prob233, Prob234, Prob235, Prob236, Prob237, Prob238, Prob239, Prob240, Prob241, Prob242, Prob243, Prob244, Prob245, Prob246, Prob247, Prob248, Prob249, Prob250, Prob251, Prob252, Prob253, Prob254, Prob255, Prob256, Prob257, Prob258, Prob259, Prob260, Prob261, Prob262, Prob263, Prob264, Prob265, Prob266, Prob267, Prob268, Prob269, Prob270, Prob271, Prob272, Prob273, Prob274, Prob275, Prob276, Prob277, Prob278, Prob279, Prob280, Prob281, Prob282, Prob283, Prob284, Prob285, Prob286, Prob287, Prob288, Prob289, Prob290, Prob291, Prob292, Prob293, Prob294, Prob295, Prob296, Prob297, Prob298, Prob299, Prob300, Prob301, Prob302, Prob303, Prob304, Prob305, Prob306, Prob307, Prob308, Prob309, Prob310, Prob311, Prob312, Prob313, Prob314, Prob315, Prob316, Prob317, Prob318, Prob319, Prob320, Prob321, Prob322, Prob323, Prob324, Prob325, Prob326, Prob327, Prob328, Prob329, Prob330, Prob331, Prob332, Prob333, Prob334, Prob335, Prob336, Prob337, Prob338, Prob339, Prob340, Prob341, Prob342, Prob343, Prob344, Prob345, Prob346, Prob347, Prob348, Prob349, Prob350, Prob351, Prob352, Prob353, Prob354, Prob355, Prob356, Prob357, Prob358, Prob359, Prob360, Prob361, Prob362, Prob363, Prob364, Prob365, Prob366, Prob367, Prob368, Prob369, Prob370, Prob371, Prob372, Prob373, Prob374, Prob375, Prob376, Prob377, Prob378, Prob379, Prob380, Prob381, Prob382, Prob383, Prob384, Prob385, Prob386, Prob387, Prob388, Prob389, Prob390, Prob391, Prob392, Prob393, Prob394, Prob395, Prob396, Prob397, Prob398, Prob399, Prob400, Prob401, Prob402, Prob403, Prob404, Prob405, Prob406, Prob407, Prob408, Prob409, Prob410, Prob411, Prob412, Prob413, Prob414, Prob415, Prob416, Prob417, Prob418, Prob419, Prob420, Prob421, Prob422, Prob423, Prob424, Prob425, Prob426, Prob427, Prob428, Prob429, Prob430, Prob431, Prob432, Prob433, Prob434, Prob435, Prob436, Prob437, Prob438, Prob439, Prob440, Prob441, Prob442, Prob443, Prob444, Prob445, Prob446, Prob447, Prob448, Prob449, Prob450, Prob451, Prob452, Prob453, Prob454, Prob455, Prob456, Prob457, Prob458, Prob459, Prob460, Prob461, Prob462, Prob463, Prob464, Prob465, Prob466, Prob467, Prob468, Prob469, Prob470, Prob471, Prob472, Prob473, Prob474, Prob475, Prob476, Prob477, Prob478, Prob479, Prob480, Prob481, Prob482, Prob483, Prob484, Prob485, Prob486, Prob487, Prob488, Prob489, Prob490, Prob491, Prob492, Prob493, Prob494, Prob495, Prob496, Prob497, Prob498, Prob499, Prob500, Prob501, Prob502, Prob503, Prob504, Prob505, Prob506, Prob507, Prob508, Prob509, Prob510, Prob511, Prob512, Prob513, Prob514, Prob515, Prob516, Prob517, Prob518, Prob519, Prob520, Prob521, Prob522, Prob523, Prob524, Prob525, Prob526, Prob527, Prob528, Prob529, Prob530, Prob531, Prob532, Prob533, Prob534, Prob535, Prob536, Prob537, Prob538, Prob539, Prob540, Prob541, Prob542, Prob543, Prob544, Prob545, Prob546, Prob547, Prob548, Prob549, Prob550, Prob551, Prob552, Prob553, Prob554, Prob555, Prob556, Prob557, Prob558, Prob559, Prob560, Prob561, Prob562, Prob563, Prob564, Prob565, Prob566, Prob567, Prob568, Prob569, Prob570, Prob571, Prob572, Prob573, Prob574, Prob575, Prob576, Prob577, Prob578, Prob579, Prob580, Prob581, Prob582, Prob583, Prob584, Prob585, Prob586, Prob587, Prob588, Prob589, Prob590, Prob591, Prob592, Prob593, Prob594, Prob595, Prob596, Prob597, Prob598, Prob599, Prob600, Prob601, Prob602, Prob603, Prob604, Prob605, Prob606, Prob607, Prob608, Prob609, Prob610, Prob611, Prob612, Prob613, Prob614, Prob615, Prob616, Prob617, Prob618, Prob619, Prob620, Prob621, Prob622, Prob623, Prob624, Prob625, Prob626, Prob627, Prob628, Prob629, Prob630, Prob631, Prob632, Prob633, Prob634, Prob635, Prob636, Prob637, Prob638, Prob639, Prob640, Prob641, Prob642, Prob643, Prob644, Prob645, Prob646, Prob647, Prob648, Prob649, Prob650, Prob651, Prob652, Prob653, Prob654, Prob655, Prob656, Prob657, Prob658, Prob659, Prob660, Prob661, Prob662, Prob663, Prob664, Prob665, Prob666, Prob667, Prob668, Prob669, Prob670, Prob671, Prob672, Prob673, Prob674, Prob675, Prob676, Prob677, Prob678, Prob679, Prob680, Prob681, Prob682, Prob683, Prob684, Prob685, Prob686, Prob687, Prob688, Prob689, Prob690, Prob691, Prob692, Prob693, Prob694, Prob695, Prob696, Prob697, Prob698, Prob699, Prob700, Prob701, Prob702, Prob703, Prob704, Prob705, Prob706, Prob707, Prob708, Prob709, Prob710, Prob711, Prob712, Prob713, Prob714, Prob715, Prob716, Prob717, Prob718, Prob719, Prob720, Prob721, Prob722, Prob723, Prob724, Prob725, Prob726, Prob727, Prob728, Prob729, Prob730, Prob731, Prob732, Prob733, Prob734, Prob735, Prob736, Prob737, Prob738, Prob739, Prob740, Prob741, Prob742, Prob743, Prob744, Prob745, Prob746, Prob747, Prob748, Prob749, Prob750, Prob751, Prob752, Prob753, Prob754, Prob755, Prob756, Prob757, Prob758, Prob759, Prob760, Prob761, Prob762, Prob763, Prob764, Prob765, Prob766, Prob767, Prob768, Prob769, Prob770, Prob771, Prob772, Prob773, Prob774, Prob775, Prob776, Prob777, Prob778, Prob779, Prob780 from ProbListP7R195Z140mm
where MinRun <= {1} and {1} <= MaxRun
order by PointID
'''.format(conf_label, abs(run_number))
    data = pd.read_sql_query(sql, conn)
    conn.close()

    return data
