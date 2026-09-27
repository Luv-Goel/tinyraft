# Contributing to TinyRaft

First off, thank you for considering contributing to TinyRaft! It's people like you that make TinyRaft such a great tool.

## Where do I go from here?

If you've noticed a bug or have a feature request, make sure to check our [Issues](https://github.com/Luv-Goel/tinyraft/issues) if it's already there. If not, open a new issue!

## Fork & create a branch

If this is something you think you can fix, then fork TinyRaft and create a branch with a descriptive name.

A good branch name would be (where issue #325 is the ticket you're working on): `issue-325-add-election-timeout`

## Get the test suite running

Make sure you're using Python 3.8+.
```sh
pip install -e .[test]
pytest
```

## Implement your fix or feature

At this point, you're ready to make your changes! Feel free to ask for help; everyone is a beginner at first.

## Make a Pull Request

At this point, you should switch back to your master branch and make sure it's up to date with TinyRaft's master branch.
Then create a Pull Request against the `master` or `main` branch of TinyRaft.
