
def init(engine):
    from alchemy_crud_example.orm.models import Base

    Base.metadata.create_all(engine)

    for t in Base.metadata.tables:
        print(Base.metadata.tables[t])