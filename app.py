BuildError
werkzeug.routing.exceptions.BuildError: Could not build url for endpoint 'api_catalogos_import'. Did you mean 'admin_catalogos' instead?

Traceback (most recent call last)
File "/opt/render/project/src/.venv/lib/python3.11/site-packages/flask/app.py", line 1498, in __call__
return self.wsgi_app(environ, start_response)
       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/opt/render/project/src/.venv/lib/python3.11/site-packages/flask/app.py", line 1476, in wsgi_app
response = self.handle_exception(e)
           ^^^^^^^^^^^^^^^^^^^^^^^^
File "/opt/render/project/src/.venv/lib/python3.11/site-packages/flask/app.py", line 1473, in wsgi_app
response = self.full_dispatch_request()
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/opt/render/project/src/.venv/lib/python3.11/site-packages/flask/app.py", line 882, in full_dispatch_request
rv = self.handle_user_exception(e)
     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/opt/render/project/src/.venv/lib/python3.11/site-packages/flask/app.py", line 880, in full_dispatch_request
rv = self.dispatch_request()
     ^^^^^^^^^^^^^^^^^^^^^^^
File "/opt/render/project/src/.venv/lib/python3.11/site-packages/flask/app.py", line 865, in dispatch_request
return self.ensure_sync(self.view_functions[rule.endpoint])(**view_args)  # type: ignore[no-any-return]
       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/opt/render/project/src/app.py", line 313, in admin_catalogos
return render_template("admin_catalogos.html", title="Admin Catálogos",
       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/opt/render/project/src/app.py", line 1083, in render_template
return _real_render_template(name, **ctx)
       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/opt/render/project/src/.venv/lib/python3.11/site-packages/flask/templating.py", line 150, in render_template
return _render(app, template, context)
       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/opt/render/project/src/.venv/lib/python3.11/site-packages/flask/templating.py", line 131, in _render
rv = template.render(context)
     ^^^^^^^^^^^^^^^^^^^^^^^^
File "/opt/render/project/src/.venv/lib/python3.11/site-packages/jinja2/environment.py", line 1295, in render
self.environment.handle_exception()
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/opt/render/project/src/.venv/lib/python3.11/site-packages/jinja2/environment.py", line 942, in handle_exception
raise rewrite_traceback_stack(source=source)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/opt/render/project/src/templates/admin_catalogos.html", line 1, in top-level template code
{% extends "base.html" %}
File "/opt/render/project/src/templates/base.html", line 79, in top-level template code
{% block content %}{% endblock %}
File "/opt/render/project/src/templates/admin_catalogos.html", line 12, in block 'content'
<form action="{{ url_for('api_catalogos_import') }}" method="POST" enctype="multipart/form-data">
File "/opt/render/project/src/.venv/lib/python3.11/site-packages/flask/app.py", line 1084, in url_for
return self.handle_url_build_error(error, endpoint, values)
       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/opt/render/project/src/.venv/lib/python3.11/site-packages/flask/app.py", line 1073, in url_for
rv = url_adapter.build(  # type: ignore[union-attr]
     
File "/opt/render/project/src/.venv/lib/python3.11/site-packages/werkzeug/routing/map.py", line 924, in build
raise BuildError(endpoint, values, method, self)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
werkzeug.routing.exceptions.BuildError: Could not build url for endpoint 'api_catalogos_import'. Did you mean 'admin_catalogos' instead?
