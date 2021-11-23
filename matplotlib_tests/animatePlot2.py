import matplotlib.pylab as plt
import matplotlib.animation as animation
import numpy as np


# create image with format (time,x,y)
image = np.random.rand(100, 10, 10)

# setup figure
fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)

# set up viewing window (in this case the 25 most recent values)
repeat_length = (np.shape(image)[0]+1)/4
ax.set_xlim([0, repeat_length])
# ax.autoscale_view()
ax.set_ylim([np.amin(image[:, 5, 5]),np.amax(image[:, 5, 5])])

# set up list of images for animation


# im = ax1.imshow(image[0,:,:])
im2, = ax.plot([], [], color=(0, 0, 1))

def func(n):
    # im.set_data(image[n,:,:])

    im2.set_xdata(np.arange(n))
    im2.set_ydata(image[0:n, 5, 5])
    if n>repeat_length:
        lim = ax.set_xlim(n-repeat_length, n)
    else:
        # makes it look ok when the animation loops
        lim = ax.set_xlim(0, repeat_length)
    return im2

ani = animation.FuncAnimation(fig, func, frames=image.shape[0], interval=1000, blit=False)

plt.show()