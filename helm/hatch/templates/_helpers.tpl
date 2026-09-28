{{/*
An init container that holds a pod back until each dependency it names, from
postgres, redis and kafka, accepts connections. The services connect as they
start and exit if they cannot, so without it they crash-loop while the
infrastructure comes up. Call it with the root context, then the names:

  {{- include "hatch.waitFor" (list . "postgres" "redis") | nindent 8 }}
*/}}
{{- define "hatch.waitFor" -}}
{{- $root := first . -}}
- name: wait-for-deps
  image: {{ $root.Values.waitImage }}
  command: ["/bin/sh", "-c"]
  args:
    - |
      {{- range rest . }}
      until nc -w 2 {{ . }} {{ (index $root.Values .).port }} </dev/null 2>/dev/null; do
        echo "waiting for {{ . }}"
        sleep 2
      done
      {{- end }}
{{- end }}
