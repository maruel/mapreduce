#!/usr/bin/env python
# Copyright 2014 Marc-Antoine Ruel. All rights reserved.
# Use of this source code is governed under the Apache License, Version 2.0
# that can be found in the LICENSE file.

"""Runs complete presubmit checks on this package."""

import logging
import optparse
import os
import subprocess
import sys
import time


THIS_FILE = os.path.abspath(__file__)
ROOT_DIR = os.path.dirname(THIS_FILE)


def call(cmd, reldir):
  logging.info('cwd=%-16s; %s', reldir, ' '.join(cmd))
  return subprocess.Popen(
      cmd, cwd=os.path.join(ROOT_DIR, reldir),
      stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


def drain(proc):
  if not proc:
    return 'Process failed'
  out = proc.communicate()[0]
  if proc.returncode:
    return out


def check_or_install(tool, *urls):
  try:
    # There's no .go files in git-hooks.
    return call(tool, 'git-hooks')
  except OSError:
    for url in urls:
      print('Warning: installing %s' % url)
      subprocess.check_call(['go', 'get', '-u', url])
    return call(tool, 'git-hooks')


def main():
  parser = optparse.OptionParser(description=sys.modules[__name__].__doc__)
  parser.add_option(
      '-v', '--verbose', action='store_true', help='Logs what is being run')
  parser.add_option(
      '--gofmt', action='store_true', help=optparse.SUPPRESS_HELP)
  parser.add_option(
      '--goimports', action='store_true', help=optparse.SUPPRESS_HELP)
  parser.add_option(
      '--golint', action='store_true', help=optparse.SUPPRESS_HELP)
  parser.add_option(
      '--govet', action='store_true', help=optparse.SUPPRESS_HELP)
  options, args = parser.parse_args()
  if args:
    parser.error('Unknown args: %s' % args)

  if options.gofmt:
    # gofmt doesn't return non-zero even if some files need to be updated.
    out = subprocess.check_output(['gofmt', '-l', '-s', '.'])
    if out:
      print('These files are improperly formmatted. Please run: gofmt -w -s .')
      sys.stdout.write(out)
      return 1
    return 0

  if options.goimports:
    # goimports doesn't return non-zero even if some files need to be updated.
    out = subprocess.check_output(['goimports', '-l', '.'])
    if out:
      print('These files are improperly formmatted. Please run: goimports -w .')
      sys.stdout.write(out)
      return 1
    return 0

  if options.golint:
    # golint doesn't return non-zero ever.
    out = subprocess.check_output(['golint', '.'])
    if out:
      print('These files are not golint free.')
      sys.stdout.write(out)
      return 1
    return 0

  if options.govet:
    # govet is very noisy about "composite literal uses unkeyed fields" which
    # cannot be turned off so strip these and ignore the return code.
    proc = subprocess.Popen(
        ['go', 'tool', 'vet', '-all', '.'],
        stdout=subprocess.PIPE)
    out = '\n'.join(
        l for l in proc.communicate()[0].splitlines()
        if not l.endswith(' composite literal uses unkeyed fields'))
    if out:
      print(out)
      return 1
    return 0

  logging.basicConfig(
      level=logging.DEBUG if options.verbose else logging.ERROR,
      format='%(levelname)-5s: %(message)s')

  start = time.time()
  procs = [
    check_or_install(['errcheck'], 'github.com/kisielk/errcheck'),
    check_or_install(
        ['goimports', '.'],
        'code.google.com/p/go.tools/cmd/cover',
        'code.google.com/p/go.tools/cmd/goimports',
        'code.google.com/p/go.tools/cmd/vet'),
    check_or_install(['golint'], 'github.com/golang/lint/golint'),
  ]
  while procs:
    drain(procs.pop(0))
  logging.info('Prerequisites check completed.')

  procs = [
    call(['go', 'test'], '.'),
    call(['errcheck', 'github.com/maruel/mapreduce'], '.'),
    call([sys.executable, THIS_FILE, '--goimports'], '.'),
    # TODO(maruel): Likely always redundant with goimports.
    call([sys.executable, THIS_FILE, '--gofmt'], '.'),

    # There starts the cheezy part that may return false positives. I'm sorry
    # David.
    call([sys.executable, THIS_FILE, '--golint'], '.'),
    call([sys.executable, THIS_FILE, '--govet'], '.'),
  ]
  failed = False
  out = drain(procs.pop(0))
  if out:
    failed = True
    print out

  for p in procs:
    out = drain(p)
    if out:
      failed = True
      print out

  end = time.time()
  if failed:
    print('Presubmit checks failed in %1.3fs!' % (end-start))
    return 1
  print('Presubmit checks succeeded in %1.3fs!' % (end-start))
  return 0


if __name__ == '__main__':
  sys.exit(main())
