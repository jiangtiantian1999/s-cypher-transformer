import os
from unittest import TextTestRunner, TestSuite, defaultTestLoader

if __name__ == '__main__':
    current_path = os.path.dirname(__file__)
    IC_suite = defaultTestLoader.discover(current_path, "test_IC.py")
    IS_suite = defaultTestLoader.discover(current_path, "test_IS.py")
    INS_DEL_suite = defaultTestLoader.discover(current_path, "test_INS_DEL.py")
    BI_suite = defaultTestLoader.discover(current_path, "test_IS.py")
    spath_suite = defaultTestLoader.discover(current_path, "test_spath.py")

    suite = TestSuite()
    suite.addTest(IC_suite)
    suite.addTest(IS_suite)
    suite.addTest(INS_DEL_suite)
    suite.addTest(BI_suite)

    runner = TextTestRunner()
    runner.run(suite)
