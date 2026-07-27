data "external_schema" "gorm" {
  program = [
    "go",
    "run",
    "-mod=mod",
    "./utils/atlas-migrate",
  ]
}
env "gorm" {
  src = data.external_schema.gorm.url
  dev = "docker://postgres/18/dev"
  migration {
    dir = "file://migrations"
  }
  format {
    migrate {
      diff = "{{ sql . \"  \" }}"
    }
  }
}
