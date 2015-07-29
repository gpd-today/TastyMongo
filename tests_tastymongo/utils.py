


class Struct( object ):
    def __init__( self, **entries ):
        self.__dict__.update( entries )

    def __eq__( self, other ):
        return self.__dict__ == other.__dict__

    def __ne__( self, other ):
        return not self.__eq__( other )

