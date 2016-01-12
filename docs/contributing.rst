.. _contributing:

Contributing
============

There are many ways to contribute to Exporters project. Here are some of them:

    - Report bugs and request features in the issue tracker, trying to follow the guidelines
    detailed in Reporting bugs below.
    - Submit patches for new functionality and/or bug fixes. Please read Writing patches
    and Submitting patches below for details on how to write and submit a patch.


Reporting bugs
~~~~~~~~~~~~~~

Well-written bug reports are very helpful, so keep in mind the following guidelines when reporting a new bug.

    - check the open issues to see if it has already been reported. If it has, don’t dismiss
    the report but check the ticket history and comments, you may find additional useful information to contribute.
    - write complete, reproducible, specific bug reports. The smaller the test case, the
    better. Remember that other developers won’t have your project to reproduce the bug,
    so please include all relevant files required to reproduce it.



Writing patches
~~~~~~~~~~~~~~~

The better written a patch is, the higher chance that it’ll get accepted and the sooner that will be merged.

Well-written patches should:

    - contain the minimum amount of code required for the specific change. Small patches
    are easier to review and merge. So, if you’re doing more than one change (or bug fix),
    please consider submitting one patch per change. Do not collapse multiple changes into
    a single patch. For big changes consider using a patch queue.
    - pass all unit-tests. See Running tests below.
    - include one (or more) test cases that check the bug fixed or the new functionality
    added. See Writing tests below.
    - if you’re adding or changing a public (documented) API, please include the documentation
    changes in the same patch. See Documentation policies below.


Submitting patches
~~~~~~~~~~~~~~~~~~

The best way to submit a patch is to issue a pull request on Github, optionally creating a
new issue first.

Remember to explain what was fixed or the new functionality (what it is, why it’s needed, etc).
The more info you include, the easier will be for core developers to understand and accept your patch.

You can also discuss the new functionality (or bug fix) before creating the patch, but it’s
always good to have a patch ready to illustrate your arguments and show that you have put
some additional thought into the subject. A good starting point is to send a pull request
on Github. It can be simple enough to illustrate your idea, and leave documentation/tests
for later, after the idea has been validated and proven useful.

Finally, try to keep aesthetic changes (PEP 8 compliance, unused imports removal, etc) in
separate commits than functional changes. This will make pull requests easier to review
and more likely to get merged.


Coding style
~~~~~~~~~~~~

Please follow these coding conventions when writing code for inclusion in Exporters:

    - Unless otherwise specified, follow PEP 8.
    - It’s OK to use lines longer than 80 chars if it improves the code readability.
    - Don’t put your name in the code you contribute. Our policy is to keep the contributor’s
    name in the AUTHORS file distributed with Exporters.


Tests
~~~~~
Running tests requires tox.

Running tests
*************

Make sure you have a recent enough tox installation:

.. code-block:: shell

    tox --version

If your version is older than 1.7.0, please update it first:

.. code-block:: shell

    pip install -U tox

To run all tests go to the root directory of Scrapy source code and run:

.. code-block:: shell

    tox

To run a specific test (say tests/test_filters.py) use:

.. code-block:: shell

    tox -- tests/test_filters.py

To see coverage report install coverage (pip install coverage) and run:

.. code-block:: shell

    coverage report

see output of coverage --help for more options like html or xml report.


Writing tests
*************

All functionality (including new features and bug fixes) must include a test case to check
that it works as expected, so please include tests for your patches if you want them to get
accepted sooner.

Exporters uses unit-tests, which are located in the tests/ directory. Their module name
typically resembles the full path of the module they’re testing.