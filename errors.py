# -*- coding: utf-8 -*-
"""Custom exceptions for cubely"""

class Error(Exception):
    pass

class DatabaseError(Error):
    """Exception raised for errors in database manipulation.

    Attributes:
        expr -- input expression in which the error occurred
        msg -- explanation of the error
    """

    def __init__(self, expr, msg):
        self.expr = expr
        self.msg = msg


class DimensionError(Error):
    def __init__(self, expr, msg):
        self.expr = expr
        self.msg = msg


class PositionError(Error):
    def __init__(self, expr, msg):
        self.expr = expr
        self.msg = msg

class PositionAlreadyExistsError(PositionError):
    pass

class CubeError(Error):
    def __init__(self, expr, msg):
        self.expr = expr
        self.msg = msg


class HierarchyError(Error):
    def __init__(self, expr, msg):
        self.expr = expr
        self.msg = msg


class FormulaError(Error):
    def __init__(self, expr, msg):
        self.expr = expr
        self.msg = msg


class RelationError(Error):
    def __init__(self, expr, msg):
        self.expr = expr
        self.msg = msg