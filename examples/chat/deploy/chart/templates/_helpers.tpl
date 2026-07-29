{{/* Suffixes (-silo, -redis, -web) get appended — keep the base short. */}}
{{- define "chat.fullname" -}}
{{- printf "%s" .Release.Name | trunc 40 | trimSuffix "-" -}}
{{- end -}}

{{- define "chat.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "chat.siloSelector" -}}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: silo
{{- end -}}

{{- define "chat.redisSelector" -}}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: redis
{{- end -}}
