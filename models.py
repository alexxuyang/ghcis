"""业务实体。"""

from pony.orm import Optional, PrimaryKey, Required

from database import db


class AdmissionOffer(db.Entity):
    """毕业生预录取/入读展开行：届别 + 地区 + 学校 + 该校总份数 + 学生 + 该生份数。"""

    id = PrimaryKey(int, auto=True)
    cohort = Required(int)
    region = Required(str)
    school_cn = Required(str)
    school_en = Optional(str)
    university_total_offers = Required(int)
    student_name = Required(str)
    student_offers = Required(int)
