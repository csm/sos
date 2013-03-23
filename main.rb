# A partially S3-compatible object store.
# Copyright (C) 2013 Memeo, Inc.

require 'sinatra'
require 'builder'
require 'pairtree'
require 'uri'

# TODO configuration here
basedir = '/Users/csm/sos'

get '/' do
	xm = Builder::XmlMarkup.new()
	xm.ListBucketsResult {
		xm.Buckets {
			Dir.entries(basedir).select { |d| d != '.' && d != '..' }.each {
				|d| xm.Bucket {
					xm.Name URI::unescape(d)
					xm.CreationDate File.ctime(File.join(basedir, d)).iso8601
				}
			}
		}
	}
end

put '/:bucket' do
	path = File.join(basedir, URI::escape(params[:bucket]))
	if File.exists? path
		status 409
	else
		pt = Pairtree.at(path, :create => true)
		status 201
	end
end

delete '/:bucket' do
	path = File.join(basedir, URI::escape(params[:bucket]))
	if File.exists? path
		FileUtils.rm_rf path # TODO something else, filter .. etc.
	else
		status 404
	end
end

get '/:bucket' do
	path = File.join(basedir, URI::escape(params[:bucket]))
	if File.exists? path
		delim = params.fetch('delimiter', nil)
		marker = params.fetch('marker', nil)
		maxkeys = params.fetch('max-keys', 1000).to_i
		prefix = params.fetch('prefix', nil)
		pt = Pairtree.at(path)
		xm = Builder::XmlMarkup.new()
		xm.ListBucketResult {
			xm.Name params[:bucket]
			pt.list.select { |id| (!marker or id >= marker) }.each {
				|id| if maxkeys > 0
					xm.Contents {
						obj = pt.get(id)
						xm.Name(id)
						xm.LastModified(obj.stat('data').mtime.iso8601)
						xm.Size(obj.stat('data').size.to_s)
					}
				else
					xm.IsTrunncated("true")
					xm.Marker(id)
					break
				end
				maxkeys = maxkeys - 1
			}
		}
	else
		status 404
	end
end

put '/:bucket/:object' do
	path = File.join(basedir, URI::escape(params[:bucket]))
	if File.exists? path
		pt = Pairtree.at(path)
		if pt.exists? params[:object]
			s = 200
			obj = pt.get(params[:object])
		else
			s = 201
			obj = pt.mk(params[:object])
		end
		obj.open('data', 'w') do |f|
			f.write(request.body.read)
		end
		status s
		''
	else
		status 404
	end
end

get '/:bucket/:object' do
	path = File.join(basedir, URI::escape(params[:bucket]))
	if File.exists? path
		pt = Pairtree.at(path)
		if pt.exists? params[:object]
			obj = pt.get(params[:object])
			status 200
			s = obj.open('data', 'r') do |s|
				body s.read()
			end
		else
			status 404
		end
	else
		status 404
	end
end

delete '/:bucket/:object' do
	path = File.join(basedir, URI::escape(params[:bucket]))
	if File.exists? path
		pt = Pairtree.at(path)
		if pt.exists? params[:object]
			pt.purge! params[:object]
		else
			status 404
		end
	else
		status 404
	end
end