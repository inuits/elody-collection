import werkzeug


def get_filename(req):
    req.add_argument(
        "filename",
        required=False,
        type=werkzeug.datastructures.FileStorage,
        help="filename to be " "upload",
    )
    parse_args = req.parse_args()
    file_name = parse_args.get("filename").filename
    file_ext = file_name.split(".")[1]
    return {"filename": file_name, "file_extension": file_ext}
