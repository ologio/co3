from colorama import Fore, Back, Style


def color_text(text, *colorama_args):
    return f"{''.join(colorama_args)}{text}{Style.RESET_ALL}"

def text_mod(text, *colorama_args, pad=0):
    return color_text(text.ljust(pad), *colorama_args)

