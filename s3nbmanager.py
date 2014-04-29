"""A notebook manager that uses the local file system for storage.

Authors:

* Brian Granger
* Zach Sailer

Modified by Cesar Matos
"""


#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------

import io
import os
import glob
import shutil

from tornado import web

import boto
from boto.s3.connection import S3Connection

from IPython.html.services.notebooks.nbmanager import NotebookManager
from IPython.nbformat import current
from IPython.utils.traitlets import Unicode, Bool, TraitError
from IPython.utils.py3compat import getcwd
from IPython.utils import tz
from IPython.html.utils import is_hidden, to_os_path


def sort_key(item):
    """Case-insensitive sorting."""
    return item['name'].lower()

def get_nb_bucket_settings():
	bucket = 'rawlabs-notebooks'
	folder = 'dropbox/%s' % os.environ['DROPBOX_UID']
	if not folder.endswith('/'):
		folder += '/'

	conn = S3Connection()
	bucket = conn.get_bucket(bucket)

	# Create folder in case user is logging in for the first time
	new_key_from_string(bucket, folder, '')

	return bucket, folder

def list_keys(bucket, path, suffix=None):
	"""list all keys in a bucket and check for suffix"""

	# Apparently there is no easy way of doing this except to loop over the result
	# chek the parameters delimiter='', marker=''
	# then the list returns boto.s3.prefix.Prefix objects on matches
	files = []
	path = path.strip('/')
	for key in bucket.list(path):
		relative_path = key.name.replace(path, '').lstrip('/')
		if not relative_path:
			# Empty
			continue

		if '/' in relative_path.strip('/'):
			# Skip sub-folders
			continue

		if not suffix or relative_path.endswith(suffix):
			files.append(relative_path)
	return files

def key_exists(bucket, path):
	return bucket.get_key(path) is not None

def is_hidden(bucket, path):	
	"""Check if the filename starts with '.'.
	"""
	# TODO: Implement metadata on S3 to mark as hidden
	parts = path.split('/')
	return parts[-1].startswith('.')

def is_folder(bucket, path):
	"""Check if path ends with '/'"""
	if not path.endswith('/'):
		path += '/'
	return bucket.get_key(path) is not None

def move_key(bucket, new_name, old_name):
	new_name = new_name.strip('/')
	if key_exists(bucket, new_name):
	    raise web.HTTPError(409, u'Notebook with name already exists: %s' % new_name)

	old_key = bucket.get_key(old_name.strip('/'))
	if not old_key:
		raise web.HTTPError(409, u'Notebook with name does not exists: %s' % old_name)
	
	new_key = bucket.copy_key(new_name,bucket.name,old_name)
	bucket.delete_key(old_key)
	
def new_key_from_string(bucket, name, contents):
	key = boto.s3.key.Key(bucket)
	key.key = name
	key.set_contents_from_string(contents)

#-----------------------------------------------------------------------------
# Classes
#-----------------------------------------------------------------------------

class S3NotebookManager(NotebookManager):
    
	save_script = Bool(False, config=True,
		help="""Automatically create a Python script when saving the notebook.
		
		For easier use of import, %run and %load across notebooks, a
		<notebook-name>.py script will be created next to any
		<notebook-name>.ipynb on each save.  This can also be set with the
		short `--script` flag.
		"""
	)	

	def __init__(self, *args, **kwargs):		
		self.bucket, self.notebook_dir = get_nb_bucket_settings()
		self.log.info("bucket_name = %s ,  bucket_folder = %s"% (self.bucket.name, self.notebook_dir))
		
	def _notebook_dir_changed(self, name, old, new):
		"""Do a bit of validation of the notebook dir."""
		self.notebook_dir = new

	checkpoint_dir = Unicode(config=True,
		help="""The relative location in which to keep notebook checkpoints
		
		By default, it is .ipynb_checkpoints/
		"""
	)
	def _checkpoint_dir_default(self):
		return '.ipynb_checkpoints/'
    
	def _checkpoint_dir_changed(self, name, old, new):
		"""do a bit of validation of the checkpoint dir"""

		if not new.endswith('/'):
			new += '/'

		if not key_exists(self.bucket, new):
			self.log.info("Creating checkpoint dir %s", new)
			try:
				# TODO: Create empty key instead of empty file(?)
				new_key_from_string(self.bucket, new, '')
			except:
				raise TraitError("Couldn't create checkpoint dir %r" % new)

		self.checkpoint_dir = new

	def _copy(self, src, dest):
		"""copy src to dest          
		"""
		try:
			self.bucket.copy_key(dest, self.bucket.name, src)
		except boto.exception.S3CopyError as e:
			self.log.debug("bucket copy failed for on %s failed", dest, exc_info=True)
			raise e

	def get_notebook_names(self, path=''):
		"""List all notebook names in the notebook dir and path."""
		self.log.debug("getting nb names %s" % path)
		os_path = self._get_os_path(path = path)
					
		return list_keys(self.bucket, os_path, self.filename_ext)

	def path_exists(self, path):
		"""Does the API-style path (directory) actually exist?

		Parameters
		----------
		path : string
			The path to check. This is an API path (`/` separated,
			relative to base notebook-dir).

		Returns
		-------
		exists : bool
			Whether the path is indeed a directory.
		"""		
		os_path = self._get_os_path(path=path)
		if not os_path.endswith('/'):
			os_path += '/'
		return is_folder(self.bucket, os_path)
	
	def is_hidden(self, path):
		"""Does the API style path correspond to a hidden directory or file?

		Parameters
		----------
		path : string
			The path to check. This is an API path (`/` separated,
			relative to base notebook-dir).

		Returns
		-------
		exists : bool
			Whether the path is hidden.

		"""			
		os_path = self._get_os_path(path=path)
		return is_hidden(self.bucket, os_path)

	def _get_os_path(self, name=None, path=''):
		"""Given a notebook name and a URL path, return its file system
		path.

		Parameters
		----------
		name : string
		    The name of a notebook file with the .ipynb extension
		path : string
		    The relative URL path (with '/' as separator) to the named
		    notebook.

		Returns
		-------
		path : string
		    A file system path that combines notebook_dir (location where
		    server started), the relative path, and the filename with the
		    current operating system's url.
		"""
		out_path = path.strip('/')
		nb_dir = self.notebook_dir.strip('/')

		if not out_path:
			out_path = nb_dir
		elif nb_dir:
			out_path = nb_dir + '/' + out_path

		if  name:
			out_path += '/' + name.strip('/')

		return out_path


	def notebook_exists(self, name, path=''):
		"""Returns a True if the notebook exists. Else, returns False.

		Parameters
		----------
		name : string
		    The name of the notebook you are checking.
		path : string
		    The relative path to the notebook (with '/' as separator)

		Returns
		-------
		bool
		"""

		os_path = self._get_os_path(name, path=path)
		return key_exists(self.bucket, os_path)

	# TODO: Remove this after we create the contents web service and directories are
	# no longer listed by the notebook web service.
	def list_dirs(self, path):
		"""List the directories for a given API style path."""
		os_path = self._get_os_path('', path)

		self.log.info("listing dir %s, nb_dir= %s", path, self.notebook_dir)
		if not os_path.endswith('/'):
			os_path += '/'

		if not key_exists(self.bucket, os_path):
			self.log.error("path does not exist " + os_path )
			raise web.HTTPError(404, u'directory does not exist: %r' % os_path)
		elif is_hidden(self.bucket, os_path):
			self.log.error("Refusing to serve hidden directory %s, via 404 Error" % os_path )
			raise web.HTTPError(404, u'directory does not exist: %s' % path)

		dir_names = list_keys(self.bucket, os_path, '/')
		dirs = []
		for name in dir_names:
			dir_path = self._get_os_path(name, path)
			self.log.debug('checking folder %s name =%s path =%s' % (dir_path, name, path))
			if self.should_list(dir_path) and not is_hidden(self.bucket, dir_path):		
				model = self.get_dir_model(name, path)
				dirs.append(model)

		return sorted(dirs, key=sort_key)

	# TODO: Remove this after we create the contents web service and directories are
	# no longer listed by the notebook web service.
	def get_dir_model(self, name, path=''):
		"""Get the directory model given a directory name and its API style path"""
		
		# Include '/' since path must be a folder
		os_path = self._get_os_path(name, path) + '/'

		key = self.bucket.get_key(os_path)
		
		# TODO: Check datetime format (UTC vs struct time); the original code was using tz.utcfromtimestamp(info.st_ctime)

		if not key:
			self.log.error("dir model '%s' not found"% (os_path))
			raise web.HTTPError(404, u'directory does not exist: %s/%s' % (path,name) )

		# TODO: Check if last_modified & created msy be structs or datetime objects
		return dict(name=name, path=path, last_modified=key.last_modified, created=key.last_modified, type='directory')

	def list_notebooks(self, path):
		"""Returns a list of dictionaries that are the standard model
		for all notebooks in the relative 'path'.

		Parameters
		----------
		path : str
			the URL path that describes the relative path for the
			listed notebooks

		Returns
		-------
		notebooks : list of dicts
			a list of the notebook models without 'content'
		"""
		
		notebook_names = self.get_notebook_names(path)
		notebooks = [self.get_notebook(name, path, content=False)
				        for name in notebook_names if self.should_list(name)]
		notebooks = sorted(notebooks, key=sort_key)
		return notebooks

	def get_notebook(self, name, path='', content=True):
		""" Takes a path and name for a notebook and returns its model

		Parameters
		----------
		name : str
			the name of the notebook
		path : str
			the URL path that describes the relative path for
			the notebook

		Returns
		-------
		model : dict
			the notebook model. If contents=True, returns the 'contents' 
			dict in the model as well.
		"""
		if not self.notebook_exists(name=name, path=path):
			raise web.HTTPError(404, u'Notebook does not exist: %s' % name)
		os_path = self._get_os_path(name, path)

		key = self.bucket.get_key(os_path)
		model = {}
		model['name'] = name
		model['path'] = path
		model['last_modified'] = key.last_modified
		model['created'] = key.last_modified
		model['type'] = 'notebook'
		if content:
			nb = current.reads(key.get_contents_as_string(), u'json')
			self.mark_trusted_cells(nb, name, path)
			model['content'] = nb
		return model

	def save_notebook(self, model, name='', path=''):
		"""Save the notebook model and return the model with no content."""
		path = path.strip('/')

		self.log.info('File manager: saving notebook %s, %s'%( name, path));
		if 'content' not in model:
			raise web.HTTPError(400, u'No notebook JSON data provided')

		# One checkpoint should always exist
		if self.notebook_exists(name, path) and not self.list_checkpoints(name, path):	
			self.create_checkpoint(name, path)

		new_path = model.get('path', path)
		new_name = model.get('name', name)
		if path != new_path or name != new_name:
			self.log.info('renaming notebook %s %s->%s %s' (path, name, new_path, new_name))
			self.rename_notebook(name, path, new_name, new_path)
		
		# Save the notebook file
		self.log.debug('getting json content')
		os_path = self._get_os_path(new_name, new_path)	
		nb = current.to_notebook_json(model['content'])
		
		self.check_and_sign(nb, new_name, new_path)
		self.log.debug("checked and signed")

		if 'name' in nb['metadata']:
			nb['metadata']['name'] = u''
		try:
			self.log.debug("Autosaving notebook %s", os_path)
			new_key_from_string(self.bucket, os_path, current.writes(nb,  u'json'))
		except Exception as e:
			self.log.debug(e)
			raise web.HTTPError(400, u'Unexpected error while autosaving notebook: %s %s' % (os_path, e))

		# Save .py script as well
		if self.save_script:
			py_path = os.path.splitext(os_path)[0] + '.py'
			self.log.debug("Writing script %s", py_path)
			try:
				new_key_from_string(self.bucket, py_path,current.writes(nb, u'py')) 
			except Exception as e:
				self.log.error(e)
				raise web.HTTPError(400, u'Unexpected error while saving notebook as script: %s %s' % (py_path, e))

		return self.get_notebook(new_name, new_path, content=False)

	def update_notebook(self, model, name, path=''):
		"""Update the notebook's path and/or name"""
		new_name = model.get('name', name)
		new_path = model.get('path', path)
		if path != new_path or name != new_name:
		    self.rename_notebook(name, path, new_name, new_path)
		model = self.get_notebook(new_name, new_path, content=False)
		return model

	def delete_notebook(self, name, path=''):
		"""Delete notebook by name and path."""
		os_path = self._get_os_path(name, path)
		if not key_exists(self.bucket, os_path):
		    raise web.HTTPError(404, u'Notebook does not exist: %s' % os_path)
		
		# clear checkpoints
		for checkpoint in self.list_checkpoints(name, path):
			checkpoint_id = checkpoint['id']
			cp_path = self.get_checkpoint_path(checkpoint_id, name, path)
			if key_exists(self.bucket,cp_path):
				self.log.debug("Unlinking checkpoint %s", cp_path)			
				self.bucket.delete_key(cp_path)
		
		self.log.debug("Unlinking notebook %s", os_path)
		self.bucket.delete_key(os_path)
		

	def rename_notebook(self, old_name, old_path, new_name, new_path):
		"""Rename a notebook."""

		if new_name == old_name and new_path == old_path:
		    return
		
		new_os_path = self._get_os_path(new_name, new_path)
		old_os_path = self._get_os_path(old_name, old_path)

		if key_exists(self.bucket, new_os_path):
		    raise web.HTTPError(409, u'Notebook with name already exists: %s' % new_os_path)

		if self.save_script:
			old_py_path = os.path.splitext(old_os_path)[0] + '.py'
			new_py_path = os.path.splitext(new_os_path)[0] + '.py'

			if key_exists(self.bucket, new_py_path):
				raise web.HTTPError(409, u'Python script with name already exists: %s' % new_py_path)

		self.log.debug('moving %s to %s',new_os_path,  old_os_path)

		try:
			move_key(self.bucket, new_os_path, old_os_path)
		except Exception as e:
			self.log.error(e)
			raise web.HTTPError(500, u'Unknown error renaming notebook: %s %s' % (old_os_path, e))

		self.log.debug('moving check points')
		# Move the checkpoints
		old_checkpoints = self.list_checkpoints(old_name, old_path)
		for cp in old_checkpoints:
			checkpoint_id = cp['id']
			old_cp_path = self.get_checkpoint_path(checkpoint_id, old_name, old_path).strip('/')
			new_cp_path = self.get_checkpoint_path(checkpoint_id, new_name, new_path).strip('/')

			#TODO: check if it is a file and exists
			self.log.debug("Renaming checkpoint %s -> %s", old_cp_path, new_cp_path)
			move_key(self.bucket, new_cp_path, old_cp_path)

		# Move the .py script
		if self.save_script:
			move_key(self.bucket, new_py_path, old_py_path)
		
	# Checkpoint-related utilities

	def get_checkpoint_path(self, checkpoint_id, name, path=''):
		"""find the path to a checkpoint"""
		self.log.info("getting checkpoint path %s, %s", name, path)	
		basename, _ = os.path.splitext(name)
		filename = u"{name}-{checkpoint_id}{ext}".format(
		    name=basename,
		    checkpoint_id=checkpoint_id,
		    ext=self.filename_ext,
		)
		
		# Checkpoints are stored in relative directories
		# e.g. given:
		# folder1/notebook1.py
		# ... the checkpoint is at ...
		# folder1/<checkpoint directory>/notebook1.py   
		base_path = self._get_os_path(self.checkpoint_dir, path=path)
		full_path = base_path +'/'+ filename
		self.log.info("checkpoint path + filename %s" % full_path)
		return full_path

	def get_checkpoint_model(self, checkpoint_id, name, path=''):
		"""construct the info dict for a given checkpoint"""

		cp_path = self.get_checkpoint_path(checkpoint_id, name, path)
		key = self.bucket.get_key(cp_path)

		last_modified = key.last_modified
		info = dict(
		    id = checkpoint_id,
		    last_modified = last_modified,
		)
		return info
		
	# public checkpoint API

	def create_checkpoint(self, name, path=''):
		"""Create a checkpoint from the current state of a notebook"""

		nb_path = self._get_os_path(name, path)
		self.log.info('creating checkpoint "%s" "%s" "%s"' %(path, name, nb_path))
		# only the one checkpoint ID:
		checkpoint_id = u"checkpoint"
		cp_path = self.get_checkpoint_path(checkpoint_id, name, path)
		
		self.log.info("creating checkpoint for notebook %s", name)
		if not key_exists(self.bucket, self.checkpoint_dir):
			new_key_from_string(self.bucket, self.checkpoint_dir, '')

		self._copy(nb_path, cp_path)
		
		# return the checkpoint info
		return self.get_checkpoint_model(checkpoint_id, name, path)

	def list_checkpoints(self, name, path=''):
		"""list the checkpoints for a given notebook
		
		This notebook manager currently only supports one checkpoint per notebook.
		"""
		self.log.info("listing checkpoint %s %s", path, name)
		checkpoint_id = "checkpoint"
		os_path = self.get_checkpoint_path(checkpoint_id, name, path)
		if not key_exists(self.bucket, os_path):
		    return []
		else:
		    return [self.get_checkpoint_model(checkpoint_id, name, path)]
		

	def restore_checkpoint(self, checkpoint_id, name, path=''):
		"""restore a notebook to a checkpointed state"""

		self.log.info("restoring Notebook %s from checkpoint %s", name, checkpoint_id)
		nb_path = self._get_os_path(name, path)
		cp_path = self.get_checkpoint_path(checkpoint_id, name, path)

		if not key_exists(self.bucket, cp_path):
		    self.log.debug("checkpoint file does not exist: %s", cp_path)
		    raise web.HTTPError(404,
		        u'Notebook checkpoint does not exist: %s-%s' % (name, checkpoint_id)
		    )
		# ensure notebook is readable (never restore from an unreadable notebook)
		key = self.bucket.get_key(cp_path)
		nb = current.reads(key.get_contents_as_string(), u'json')
		self._copy(cp_path, nb_path)
		self.log.debug("copying %s -> %s", cp_path, nb_path)

	def delete_checkpoint(self, checkpoint_id, name, path=''):
		"""delete a notebook's checkpoint"""

		cp_path = self.get_checkpoint_path(checkpoint_id, name, path)
		if not key_exists(self.bucket, cp_path):
		    raise web.HTTPError(404,
		        u'Notebook checkpoint does not exist: %s%s-%s' % (path, name, checkpoint_id)
		    )
		self.log.debug("unlinking %s", cp_path)
		self.bucket.delete_key(cp_path)


	def info_string(self):
		return "Serving notebooks from local directory: %s" % self.notebook_dir
