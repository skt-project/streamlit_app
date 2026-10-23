import mobile


def test_camera_button_is_relabelled_red_with_white_bold_text():
    css = mobile._CSS
    block = css[css.index('[data-testid="stCameraInputButton"] {'):]
    assert "#d32f2f" in block.split("}")[0]                       # latar merah
    assert "color: #fff" in block.split("}")[0]                    # huruf putih
    after = css[css.index('stCameraInputButton"]::after'):].split("}")[0]
    assert 'content: "Ambil gambar"' in after and "font-weight: 700" in after and "color: #fff" in after


def test_clear_photo_button_is_not_targeted():
    # hanya testid tombol ambil gambar yang diubah
    assert mobile._CSS.count("stCameraInputButton") >= 1
    assert "stCameraInputSwitchButton" not in mobile._CSS


def test_delete_button_is_white_bold_text_on_red_and_placeholder_fits():
    css = mobile._CSS
    block = css[css.index('st-key-del_"] button {'):].split("}")[0]
    assert "background: #d32f2f" in block and "color: #fff" in block and "font-weight: 700" in block
    assert 'input[aria-label="Sell Out"]::placeholder' in css and 'input[aria-label="Stock"]::placeholder' in css
